pub mod raft_state {

extern crate common_lib;


use common_lib::error::error::CommonError;

use log::{error, info};

use serde_derive::{Serialize, Deserialize};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use std::io::ErrorKind;
use std::collections::HashMap;
use chrono::Utc;
use rand::Rng;

type RaftTerm = u64;
type RaftLogIndex = usize;
type RaftNodeId = String;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RafCommand {
    pub id: String
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RaftLogEntry {
    pub command: RafCommand,
    pub term: RaftTerm
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Copy, Clone)]
pub enum RaftWho {
    Follower,
    Candidate,
    Leader
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RaftState {
    //Persistent state on all servers:
    //(Updated on stable storage before responding to RPCs)
    term: RaftTerm, //latest term server has seen (initialized to 0n first boot, increases monotonically)
    voted_for: Option<RaftNodeId>, //candidateId that received vote in current term (or null if none)
    pub log: Vec<RaftLogEntry>, /*log entries; each entry contains command
                                for state machine, and term when entry
                                was received by leader (first index is 1)*/

    //Volatile state on all servers:
    pub commit_index: RaftLogIndex, /*index of highest log entry known to be
                            committed (initialized to 0, increases
                            monotonically)*/
    pub last_applied: RaftLogIndex, /*index of highest log entry applied to state machine (initialized to 0, increases
                            monotonically)*/

    //Volatile state on leaders:
    //(Reinitialized after election)
    pub next_index: HashMap<RaftNodeId, RaftLogIndex>, /*for each server, index of the next log entry
                                            to send to that server (initialized to leader
                                            last log index + 1)*/
    pub match_index: HashMap<RaftNodeId, RaftLogIndex>, /*for each server, index of highest log entry
                                            known to be replicated on server
                                            (initialized to 0, increases monotonically)*/


    who: RaftWho,
    pub last_election: i64,
    pub last_leader: RaftNodeId,
    pub key_value_storage: HashMap<String, String>
}

impl RaftState {
    pub fn new() -> Self {
        Self{term: 0, voted_for: None, log: Vec::new(), commit_index:0, last_applied: 0,
            next_index: HashMap::new(), match_index: HashMap::new(), who: RaftWho::Follower,
            last_election: Utc::now().timestamp_millis(), last_leader: "".to_string(),
            key_value_storage: HashMap::new()}
    }

    pub async fn to_file(&self, file_path: &str) -> Result<(), CommonError> {
        let state_str = match serde_json::to_string(self) {
            Ok(v) => v,
            Err(e) => {
                error!("serialize error {}", e);
                return Err(CommonError::new(format!("serialize error {}", e)));
            }
        };

        let mut file = match tokio::fs::File::create(file_path).await {
            Ok(v) => v,
            Err(e) => {
                error!("open error {}", e);
                return Err(CommonError::new(format!("open error {}", e)));
            }
        };
        match file.write_all(state_str.as_bytes()).await {
            Ok(v) => v,
            Err(e) => {
                error!("write_all error {}", e);
                return Err(CommonError::new(format!("write_all error {}", e)));
            }
        }
        match file.sync_data().await {
            Ok(v) => v,
            Err(e) => {
                error!("sync_data error {}", e);
                return Err(CommonError::new(format!("sync_data error {}", e)));
            }
        }
        Ok(())
    }

    pub async fn from_file(file_path: &str) -> Result<Self, CommonError> {
        let mut file = match tokio::fs::File::open(file_path).await {
            Ok(v) => v,
            Err(e) => {
                match e.kind() {
                    ErrorKind::NotFound => {
                        return Ok(Self::new())
                    }
                    _ => {
                        error!("open {} error {}", file_path, e);
                        return Err(CommonError::new(format!("open error {}", e)));        
                    }
                }
            }
        };

        let mut contents = vec![];
        match file.read_to_end(&mut contents).await {
            Ok(v) => v,
            Err(e) => {
                error!("read_to_end error {}", e);
                return Err(CommonError::new(format!("read_to_end error {}", e)));
            }
        };
        let state_str = match std::str::from_utf8(&contents) {
            Ok(v) => v,
            Err(e) => {
                error!("from_utf8 {}", e);
                return Err(CommonError::new(format!("from_utf8 error {}", e)));
            }
        };
        let state = match serde_json::from_str(&state_str) {
            Ok(v) => v,
            Err(e) => {
                error!("deserialize error {}", e);
                return Err(CommonError::new(format!("deserialize error {}", e)));
            }
        };
        Ok(state)
    }

    pub fn set_who(&mut self, who: RaftWho) {
        info!("raft: set who {:?} -> {:?}", self.who, who);
        self.who = who;
        self.reset_election()
    }

    pub fn set_term(&mut self, term: RaftTerm) {
        info!("raft: set term {} -> {}", self.term, term);
        self.term = term;
    }

    pub fn set_voted_for(&mut self, voted_for: Option<RaftNodeId>) {
        info!("raft: set voted_for {:?}", voted_for);
        self.voted_for = voted_for;
    }

    pub fn get_who(&self) -> RaftWho {
        info!("raft: get who {:?}", self.who);
        self.who
    }

    pub fn get_term(&self) -> RaftTerm {
        info!("raft: get term {}", self.term);
        self.term
    }

    pub fn get_voted_for(&self) -> Option<RaftNodeId> {
        let voted_for = match &self.voted_for {
            Some(v) => {
                Some(v.clone())
            }
            None => None
        };
        info!("raft: get voted_for {:?}", voted_for);
        voted_for
    }

    pub fn reset_election(&mut self) {
        let mut rng = rand::thread_rng();
        let now = Utc::now().timestamp_millis();
        self.last_election = now + rng.gen_range(0..300);
        self.voted_for = None;
        info!("reset election now {} last {}", now, self.last_election);
    }
}
}