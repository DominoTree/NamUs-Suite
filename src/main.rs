use std::error::Error;
use std::sync::Arc;

use serde_json::json;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::debug;
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

async fn get_case(case_id: u32, category: CaseCategory) -> Result<(), Box<dyn Error>> {
    let res = reqwest::get(format!(
        "https://www.namus.gov/api/CaseSets/NamUs/{category}/Cases/{case_id}"
    ))
    .await;
    Ok(())
}

async fn get_cases_by_state(
    state: &str,
    category: CaseCategory,
) -> Result<Vec<u32>, Box<dyn Error>> {
    // TODO: Deal with pagination (not necessary yet)
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
        .await?;

    println!("{:?}", resp);

    Ok(Vec::new())
}

async fn get_states() -> Result<Vec<String>, Box<dyn Error>> {
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
    dotenvy::dotenv()?;

    tracing_subscriber::fmt::init();

    let states = get_states().await?;

    let sema = Arc::new(Semaphore::new(PARALLEL_REQUESTS));
    let mut jhs = Vec::new();

    for state in states {
        let sema = sema.clone();
        let jh = tokio::spawn(async move {
            let sema = sema.acquire().await.unwrap();
            println!("{:?}", sema);
            get_cases_by_state(&state, CaseCategory::MissingPersons)
                .await
                .unwrap()
            // semaphore should be dropped automatically
        });
        jhs.push(jh);
    }

    let cases = Vec::<u32>::new();
    for jh in jhs {
        let resp = jh.await.unwrap();
        println!("{:?}", resp);
    }

    // we shouldn't need to bother deserializing and reserializing these
    let results = Vec::<String>::new();
    // saving the failed IDs here should be enough
    let failed = Vec::<u32>::new();

    Ok(())
}
