use dbsp::{
    operator::{FilterMap, Min},
    trace::{BatchReader, Cursor},
    utils::{Tup2, Tup3, Tup4, Tup5},
    OrdIndexedZSet, OrdZSet, RootCircuit, SchedulerError, Stream,
};
use std::iter::once;

use crate::types::*;

pub type QueryResult = Result<Stream<RootCircuit, String>, SchedulerError>;

pub fn query_1(
    cicruit: &RootCircuit,
    posts: &Stream<RootCircuit, OrdZSet<Post, Weight>>,
    comments: &Stream<RootCircuit, OrdZSet<Comment, Weight>>,
    likes: &Stream<RootCircuit, OrdZSet<Like, Weight>>,
) -> QueryResult {
    let comments_by_parents = comments.index_with(|Tup5(id, _, _, _, parent)| Tup2(*parent, *id));
    let direct_replies = posts
        .map(|Tup4(id, _, _, _)| *id)
        .join_index(&comments_by_parents, |post_id, _, comment_id| {
            once((*comment_id, *post_id))
        });
    let all_replies = cicruit.recursive(
        |subcircuit, replies: Stream<_, OrdIndexedZSet<Submission, Submission, Weight>>| {
            let direct_replies = direct_replies.delta0(subcircuit);
            let comments_by_parents = comments_by_parents.delta0(subcircuit);
            let new_replies = replies.join_index(
                &comments_by_parents,
                |_comment_id, post_id, new_comment_id| once((*new_comment_id, *post_id)),
            );
            let all_replies = direct_replies.plus(&new_replies);
            Ok(all_replies)
        },
    )?;
    let liked_comments = likes
        .map(|Tup2(_, comment_id)| *comment_id)
        .join(&all_replies, |_comment_id, _, post_id| *post_id);
    let comments_themselves = all_replies.map(|(_, post_id)| *post_id).weigh(|_, _| 10);
    let post_dates = posts.index_with(|Tup4(post_id, date, _, _)| Tup2(*post_id, *date));
    let post_scores = liked_comments
        .plus(&comments_themselves)
        .weighted_count()
        .join(&post_dates, |post_id, score, date| {
            Tup3(*score, *date, *post_id)
        });
    format_output(&post_scores)
}

pub fn query_2(
    circuit: &RootCircuit,
    comments: &Stream<RootCircuit, OrdZSet<Comment, Weight>>,
    knows: &Stream<RootCircuit, OrdZSet<Know, Weight>>,
    likes: &Stream<RootCircuit, OrdZSet<Like, Weight>>,
) -> QueryResult {
    let initial_labels =
        likes.index_with(|Tup2(user_id, comment_id)| Tup2(Tup2(*user_id, *comment_id), *user_id));
    let knows_index = knows.index();
    let labels = circuit.recursive(
        |subcircuit,
         labels: Stream<_, OrdIndexedZSet<Tup2<Person, Submission>, Person, Weight>>| {
            let initial_labels = initial_labels.delta0(subcircuit);
            let knows_index = knows_index.delta0(subcircuit);
            let likes = likes.delta0(subcircuit);
            let labels = labels
                .map_index(|(Tup2(user_id, comment_id), label)| {
                    (*user_id, Tup2(*comment_id, *label))
                })
                .join_index(
                    &knows_index,
                    |_user_id, Tup2(comment_id, label), friend_id| {
                        once((Tup2(*friend_id, *comment_id), *label))
                    },
                )
                .join_index(&likes, |Tup2(user_id, comment_id), label, _| {
                    once((Tup2(*user_id, *comment_id), *label))
                })
                .plus(&initial_labels)
                .aggregate(Min);
            Ok(labels)
        },
    )?;
    let comment_dates =
        comments.index_with(|Tup5(comment_id, date, _, _, _)| Tup2(*comment_id, *date));
    let comment_scores = labels
        .map(|(Tup2(_user_id, comment_id), label)| Tup2(*comment_id, *label))
        .weighted_count()
        .map_index(|(Tup2(comment_id, _label), count)| (*comment_id, *count * *count))
        .aggregate_linear(|x| *x)
        .join(&comment_dates, |comment_id, score, date| {
            Tup3(*score, *date, *comment_id)
        });
    format_output(&comment_scores)
}

fn format_output(
    scores: &Stream<RootCircuit, OrdZSet<Tup3<i64, Date, u64>, Weight>>,
) -> QueryResult {
    let output = scores
        .index_with(|tuple @ Tup3(_, _, id)| Tup2(*id % 10000, *tuple))
        .topk_desc(3)
        .map_index(|(key, tuple)| (*key % 100, *tuple))
        .topk_desc(3)
        .map_index(|(_, tuple)| ((), *tuple))
        .topk_desc(3)
        .integrate()
        .apply(|batch| {
            if batch.len() < 3 {
                "".into()
            } else {
                let mut cursor = batch.cursor();
                let Tup3(_, _, id1) = cursor.val().to_owned();
                cursor.step_val();
                let Tup3(_, _, id2) = cursor.val().to_owned();
                cursor.step_val();
                let Tup3(_, _, id3) = cursor.val().to_owned();
                format!("{}|{}|{}", id3, id2, id1)
            }
        });
    Ok(output)
}
