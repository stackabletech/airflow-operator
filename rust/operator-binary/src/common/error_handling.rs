use snafu::Error;

pub fn is_a_not_found_error<T: Error>(error: &T) -> bool {
    // to get a useful representation we have to go a few levels deep
    // TODO: how could we look into source errors or the complete error string more elegantly?
    // I would prefer a function to get a complete string representation here, without going deep
    let extracted_string = error
        .source()
        .and_then(|x| x.source())
        .map(|x| x.to_string())
        .map(|x| x.contains("NotFound"));
    if let Some(true_if_not_found) = extracted_string {
        true_if_not_found
    } else {
        false
    }
}
