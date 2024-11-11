use std::error::Error;

use futures::{stream, StreamExt};
use log::*;

const PARALLEL_REQUESTS: usize = 5;

async fn _get_case(state: String, category: &str) -> Result<(), Box<dyn Error>> {
    Ok(())
}

async fn get_cases_by_state(state: &str, category: &str) -> Result<(), Box<dyn Error>> {
    println!("{state}");
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
            debug!("{state:?}");
            return Err(Box::<dyn Error>::from("Missing or invalid state name"));
        }

        states.push(state_name.unwrap().to_string());
    }
    Ok(states)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let states = get_states().await?;

    let _out = stream::iter(states)
        .for_each_concurrent(PARALLEL_REQUESTS, |state| async move {
            get_cases_by_state(&state, "Missing Persons").await.unwrap();
        })
        .await;

    Ok(())
}
