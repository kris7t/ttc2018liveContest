use dbsp::utils::{Tup2, Tup4, Tup5};

pub type Weight = i64;
pub type Date = i64;
pub type Person = u64;
pub type Submission = u64;
pub type User = Tup2<Person, String>;
pub type Know = Tup2<Person, Person>;
pub type Post = Tup4<Submission, Date, String, Person>;
pub type Comment = Tup5<Submission, Date, String, Person, Submission>;
pub type Like = Tup2<Person, Submission>;
