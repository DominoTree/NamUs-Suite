use std::error::Error;
use std::sync::Arc;

use serde_json::json;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{debug, info}; // apparently these are actually blocking, but it should be fine here
use tracing_subscriber;

const PARALLEL_REQUESTS: usize = 5;

enum CaseCategory {
    MissingPersons,
    UnidentifiedPersons,
    UnclaimedPersons,
}

impl std::fmt::Display for CaseCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let out = match self {
            CaseCategory::MissingPersons => "MissingPersons",
            CaseCategory::UnidentifiedPersons => "UnidentifiedPersons",
            CaseCategory::UnclaimedPersons => "UnclaimedPersons",
        };
        write!(f, "{}", out)
    }
}

async fn output_json_lines(data: Vec<String>, outfile: &str) -> Result<(), Box<dyn Error>> {
    let mut out = String::from("[");
    for line in data {
        out += "\t";
        out += &line;
        out += ",\r\n";
    }
    out += "]";
    Ok(())
}

async fn get_case(case_id: u64, category: CaseCategory) -> Result<String, Box<dyn Error>> {
    let res = reqwest::get(format!(
        "https://www.namus.gov/api/CaseSets/NamUs/{category}/Cases/{case_id}"
    ))
    .await;
    // just returning untouched JSON body here
    Ok(res?.text().await?)
}

async fn get_cases_by_state(
    state: &str,
    category: CaseCategory,
) -> Result<Vec<u64>, Box<dyn Error>> {
    // TODO: Deal with pagination (not necessary yet as no state has more than 10,000 cases)
    let body = json!({
        "take": 10000,
        "projections": ["namus2Number"],
        "predicates": [
            {
                "field": "stateOfLastContact",
                "operator": "IsIn",
                "values": [state],
            }
        ],
    });

    let resp = reqwest::Client::new()
        .post(format!(
            "https://www.namus.gov/api/CaseSets/NamUs/{category}/Search"
        ))
        .json(&body)
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    // TODO: clean this up and handle errors
    let results_array = resp.get("results").unwrap().as_array().unwrap();

    let mut ids = Vec::new();

    // TODO: use collect:: or similar here?
    for result in results_array {
        let case_id = result.get("namus2Number").unwrap().as_u64().unwrap();
        ids.push(case_id);
    }

    Ok(ids)
}

async fn get_states_and_territories() -> Result<Vec<String>, Box<dyn Error>> {
    let mut states = Vec::new();
    let resp = reqwest::get("https://www.namus.gov/api/CaseSets/NamUs/States")
        .await?
        .json::<serde_json::Value>()
        .await?;

    if !resp.is_array() {
        debug!("{resp:?}");
        return Err(Box::<dyn Error>::from("Invalid response"));
    }

    for state in resp.as_array().unwrap() {
        let state_name = state.get("name");

        // I believe that as_string should always be safe on an existing value
        // but it doesn't hurt to test anyways
        if state_name.is_none() || !state_name.unwrap().is_string() {
            debug!("{state:?}");
            return Err(Box::<dyn Error>::from("Missing or invalid state name"));
        }

        // We cannot take a Value and simply call .to_string() on it or it will retain the quotes
        // around the string. We must first call .as_str() to turn it into an &str
        states.push(state_name.unwrap().as_str().unwrap().to_string());
    }
    Ok(states)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt::init();

    info!("Getting active states and territories");
    let states = get_states_and_territories().await?;

    let sema = Arc::new(Semaphore::new(PARALLEL_REQUESTS));
    let mut jhs = Vec::new();

    // get lists of cases
    for state in states {
        let sema = sema.clone();
        let jh = tokio::spawn(async move {
            let sema = sema.acquire().await.unwrap();
            info!("Getting cases from {state}");
            let res = get_cases_by_state(&state, CaseCategory::MissingPersons)
                .await
                .unwrap();
            info!("Found {} cases in {state}", res.len());
            res
            // semaphore should be dropped automatically
        });
        jhs.push(jh);
    }

    let mut case_ids = Vec::<u64>::new();
    for jh in jhs {
        case_ids.append(&mut jh.await?);
    }
    info!("Found {} total cases", case_ids.len());

    let mut jhs = Vec::new();

    // get individual cases
    for case_id in case_ids {
        let sema = sema.clone();
        let jh = tokio::spawn(async move {
            let sema = sema.acquire().await.unwrap();
            get_case(case_id, CaseCategory::MissingPersons)
        });
        jhs.push(jh);
    }

    // we shouldn't need to bother deserializing and reserializing these
    let results = Vec::<String>::new();
    // saving the failed IDs here should be enough
    let failed = Vec::<u64>::new();

    for jh in jhs {
        match jh.await {
            Ok(body) => results.push(body),
            Err(_e) => failed.push(1),
        };
    }

    Ok(())
}
