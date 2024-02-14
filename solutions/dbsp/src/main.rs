use dbsp::Runtime;

mod queries;
mod read_data;
mod types;

use queries::*;
use read_data::*;
use types::*;

fn main() {
    let mut timer = std::time::Instant::now();

    let change_path = std::env::var("ChangePath").unwrap_or("None".to_string());
    let run_index = std::env::var("RunIndex").unwrap_or("None".to_string());
    let sequences = std::env::var("Sequences")
        .unwrap_or("20".to_string())
        .parse::<usize>()
        .expect("Couldn't parse Sequences as an integer");
    let change_set = std::env::var("ChangeSet").unwrap_or("None".to_string());
    let query = std::sync::Arc::new(std::env::var("Query").unwrap_or("Q2".to_string()));
    let tool = std::env::var("Tool").unwrap_or("None".to_string());
    let threads = std::env::var("Threads")
        .unwrap_or("1".to_string())
        .parse::<usize>()
        .expect("Couldn't parse Threads as an integer");
    let profile = std::env::var("Profile").unwrap_or("false".to_string()) == "true";
    let path = format!("{}/", change_path);

    let (mut dbsp, (husers, hknows, hposts, hcomments, hlikes, houtput)) = {
        let query = query.clone();
        Runtime::init_circuit(threads, move |circuit| {
            let (_users, husers) = circuit.add_input_zset::<User, Weight>();
            let (knows, hknows) = circuit.add_input_zset::<Know, Weight>();
            let (posts, hposts) = circuit.add_input_zset::<Post, Weight>();
            let (comments, hcomments) = circuit.add_input_zset::<Comment, Weight>();
            let (likes, hlikes) = circuit.add_input_zset::<Like, Weight>();
            let output = if *query == "Q1" {
                query_1(&circuit, &posts, &comments, &likes)?
            } else if *query == "Q2" {
                query_2(&circuit, &comments, &knows, &likes)?
            } else {
                panic!("Unknown query {}", query)
            };
            Ok((husers, hknows, hposts, hcomments, hlikes, output.output()))
        })
        .unwrap()
    };

    println!(
        "{:?};{:?};{};{};0;\"Initialization\";\"Time\";{}",
        tool,
        query,
        change_set,
        run_index,
        timer.elapsed().as_nanos()
    );
    timer = std::time::Instant::now();

    let users_strings = load_data(&format!("{}csv-users-initial.csv", path));
    let knows_strings = load_data(&format!("{}csv-friends-initial.csv", path));
    let posts_strings = load_data(&format!("{}csv-posts-initial.csv", path));
    let comments_strings = load_data(&format!("{}csv-comments-initial.csv", path));
    let likes_strings = load_data(&format!("{}csv-likes-initial.csv", path));

    println!(
        "{:?};{:?};{};{};0;\"Load\";\"Time\";{}",
        tool,
        query,
        change_set,
        run_index,
        timer.elapsed().as_nanos()
    );
    timer = std::time::Instant::now();

    for user_strings in users_strings {
        husers.push(strings_to_user(user_strings), 1);
    }
    for know_strings in knows_strings {
        hknows.push(strings_to_know(know_strings), 1);
    }
    for post_strings in posts_strings {
        hposts.push(strings_to_post(post_strings), 1);
    }
    for comment_strings in comments_strings {
        hcomments.push(strings_to_comment(comment_strings), 1);
    }
    for like_strings in likes_strings {
        hlikes.push(strings_to_like(like_strings), 1);
    }
    dbsp.step().unwrap();

    let mut output = houtput
        .take_from_all()
        .first()
        .map(String::to_owned)
        .unwrap_or("".into());
    println!(
        "{:?};{:?};{};{};0;\"Initial\";\"Elements\";{}",
        tool, query, change_set, run_index, output
    );
    println!(
        "{:?};{:?};{};{};0;\"Initial\";\"Time\";{}",
        tool,
        query,
        change_set,
        run_index,
        timer.elapsed().as_nanos()
    );
    timer = std::time::Instant::now();

    if profile {
        dbsp.enable_cpu_profiler().unwrap();
    }

    for round in 1..(sequences + 1) {
        let filename = format!("{}change{:02}.csv", path, round);
        let changes = load_data(&filename);
        for mut change in changes {
            let collection = change.remove(0);
            match collection.as_str() {
                "Comments" => {
                    hcomments.push(strings_to_comment(change), 1);
                }
                "Friends" => {
                    hknows.push(strings_to_know(change), 1);
                }
                "Likes" => {
                    hlikes.push(strings_to_like(change), 1);
                }
                "Posts" => {
                    hposts.push(strings_to_post(change), 1);
                }
                "Users" => {
                    husers.push(strings_to_user(change), 1);
                }
                x => {
                    panic!("Invalid enum variant: {}", x);
                }
            }
        }
        dbsp.step().unwrap();

        if let Some(new_output) = houtput.take_from_all().first() {
            output = new_output.to_owned();
        }
        println!(
            "{:?};{:?};{};{};{};\"Update\";\"Elements\";{}",
            tool, query, change_set, run_index, round, output
        );
        println!(
            "{:?};{:?};{};{};{};\"Update\";\"Time\";{}",
            tool,
            query,
            change_set,
            run_index,
            round,
            timer.elapsed().as_nanos()
        );
        timer = std::time::Instant::now();

        if profile {
            dbsp.dump_profile(format!("{}/scale_{}", query, change_set)).unwrap();
        }
    }
}
