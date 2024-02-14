use crate::types::*;

pub fn load_data(filename: &str) -> Vec<Vec<String>> {
    // Standard io/fs boilerplate.
    use std::fs::File;
    use std::io::{BufRead, BufReader};

    let mut data = Vec::new();
    let file = BufReader::new(File::open(filename).expect("Could open file"));
    for readline in file.lines() {
        if let Ok(line) = readline {
            let text: Vec<String> = line.split('|').map(|x| x.to_string()).collect();
            data.push(text);
        }
    }
    data
}

pub fn strings_to_comment(comment: Vec<String>) -> Comment {
    let mut iter = comment.into_iter();
    let id = iter.next().unwrap().parse::<Submission>().unwrap();
    let ts = iter.next().unwrap();
    let mut split = ts.split_whitespace();
    let date = split.next().unwrap();
    let time = split.next().unwrap();
    let ts = format!("{}T{}+00:00", date, time);
    let ts = chrono::DateTime::parse_from_rfc3339(ts.as_str())
        .expect("Failed to parse DateTime")
        .timestamp();
    let content = iter.next().unwrap();
    let creator = iter.next().unwrap().parse::<Person>().unwrap();
    let parent = iter.next().unwrap().parse::<Submission>().unwrap();
    (id, ts, content, creator, parent).into()
}

pub fn strings_to_know(know: Vec<String>) -> Know {
    let mut iter = know.into_iter();
    let person1 = iter.next().unwrap().parse::<Person>().unwrap();
    let person2 = iter.next().unwrap().parse::<Person>().unwrap();
    (person1, person2).into()
}

pub fn strings_to_like(like: Vec<String>) -> Like {
    let mut iter = like.into_iter();
    let person = iter.next().unwrap().parse::<Person>().unwrap();
    let comment = iter.next().unwrap().parse::<Submission>().unwrap();
    (person, comment).into()
}

pub fn strings_to_post(post: Vec<String>) -> Post {
    let mut iter = post.into_iter();
    let id = iter.next().unwrap().parse::<Submission>().unwrap();
    let ts = iter.next().unwrap();
    let mut split = ts.split_whitespace();
    let date = split.next().unwrap();
    let time = split.next().unwrap();
    let ts = format!("{}T{}+00:00", date, time);
    let ts = chrono::DateTime::parse_from_rfc3339(ts.as_str())
        .expect("Failed to parse DateTime")
        .timestamp();
    let content = iter.next().unwrap();
    let creator = iter.next().unwrap().parse::<Person>().unwrap();
    (id, ts, content, creator).into()
}

pub fn strings_to_user(user: Vec<String>) -> User {
    let mut iter = user.into_iter();
    let person = iter.next().unwrap().parse::<Person>().unwrap();
    let name = iter.next().unwrap();
    (person, name).into()
}
