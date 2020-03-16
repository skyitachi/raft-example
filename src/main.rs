#[macro_use]
extern crate slog;
extern crate slog_term;
extern crate raft;

use slog::*;
use std::collections::{HashMap, VecDeque};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TryRecvError};
use std::sync::{ Arc, Mutex };
use raft::eraftpb::{ConfState, MessageType, Message};
use raft::storage::MemStorage;
use raft::{prelude::*, StateRole };

fn main() {
    let decorator = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let logger = Logger::root(
        slog_term::FullFormat::new(decorator).build().fuse(), o!()
    );
    // println!("Hello, world!");
    const NUM_NODES: u32 = 5;
    let (mut tx_vec, mut rx_vec) =(Vec::new(), Vec::new());
    for _ in 0..NUM_NODES {
        let (tx, rx) = mpsc::channel();
        tx_vec.push(tx);
        rx_vec.push(rx);
    }

    let (tx_stop, rx_stop) = mpsc::channel();
    let rx_stop = Arc::new(Mutex::new(rx_stop));

}

enum Signal {
    Terminate,
}

struct Node {
    raft_group: Option<RawNode<MemStorage>>,
    my_mailbox: Receiver<Message>,
    mailboxes: HashMap<u64, Sender<Message>>,
    kv_pairs: HashMap<u16, String>,
}

fn example_config() -> Config {
    Config {
        election_tick: 10, 
        heartbeat_tick: 3,
        ..Default::default()
    }
}

impl Node {
    fn create_raft_leader(
        id: u64,
        my_mailbox: Receiver<Message>,
        mailboxes: HashMap<u64, Sender<Message>>,
        logger: &slog::Logger,
    ) -> Self {
        let mut cfg = example_config();
        cfg.id = id;
        let logger = logger.new(o!("tag" => format!("peer{}", id)));
        let storage = MemStorage::new_with_conf_state(ConfState::from((vec![id], vec![])));
        let raft_group = Some(RawNode::new(&cfg, storage).unwrap().with_logger(&logger));
        Node {
            raft_group,
            my_mailbox,
            mailboxes,
            kv_pairs: Default::default(),
        }

    }
    
    fn create_raft_follower(
        my_mailbox: Receiver<Message>,
        mailboxes: HashMap<u64, Sender<Message>>,
    ) -> Self {
        Node {
            raft_group: None,
            my_mailbox,
            mailboxes,
            kv_pairs: Default::default(),
        }
    }

    fn initilize_raft_from_message(&mut self, msg: &Message, logger: &slog::Logger) {
        if !is_initial_msg(msg) {
            
        }
    }
}

fn is_initial_msg(msg: &Message) -> bool {
    let msg_type = msg.get_msg_type();
    msg_type == MessageType::MsgRequestVote
        || msg_type == MessageType::MsgRequestPreVote
        || (msg_type == MessageType::MsgHeartbeat && msg.commit == 0)

}