use std::error::Error;

use log::*;

const PARALLEL_REQUESTS: u8 = 5;

async fn get_case(state: &str, category: &str) -> Result<(), Box<dyn Error>> {
    Ok(())
}

async fn get_cases_by_state(state: &str, category: &str) -> Result<(), Box<dyn Error>> {
    Ok(())
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
            return Err(Box::<dyn Error>::from("Missing or invalid state name"));
        }

        states.push(state_name.unwrap().to_string());
    }
    Ok(states)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut output = String::new();
    let states = get_states().await?;
    println!("{:?}", states);
    Ok(())
}
