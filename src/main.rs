use polars::prelude::*;
use polars_io::json::{JsonFormat, JsonReader};
use reqwest::blocking::Client;
use serde::Deserialize;

const API_TOKEN: &'static str = "0b4ad4965dc4cd6f03bc4964b9048afab60afc4b";

#[derive(Deserialize)]
struct FilingRegistrant {
    id: u16,
    name: String,
}
#[derive(Deserialize)]
struct FilingClient {
    id: u16,
    name: String
}
#[derive(Deserialize)]
struct Filing {
    filing_uuid: String,
    filing_year: u8,
    income: Option<isize>,
    expenses: Option<isize>,
    registrant: FilingRegistrant,
    client: FilingClient

}
#[derive(Deserialize)]
struct FilingResponse {
    results: Vec<Filing>
}

fn main() -> Result<(), reqwest::Error> {
    let client = Client::new();
    let url: &'static str = "https://lda.senate.gov/api/v1/filings";

    let response: FilingResponse = client.get(url).send()?.json()?;
    let series_vec: Vec<Series> = Vec::new();

    response.results.iter().for_each(|f| series_vec.push(Series::new());

    println!("Data :\n {}", df);
    Ok(())
}
