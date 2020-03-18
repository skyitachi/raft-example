extern crate slog;
extern crate slog_term;
extern crate raft;

use slog::*;
use std::collections::{HashMap, VecDeque};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TryRecvError};
use std::sync::{ Arc, Mutex };
use std::thread;

use protobuf::Message as PbMessage;

use std::time::{Instant, Duration};
use raft::eraftpb::ConfState;
use raft::storage::MemStorage;
use raft::{prelude::*, StateRole };

fn main() {
    let decorator = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let logger = Logger::root(
        slog_term::FullFormat::new(decorator).build().fuse(), o!()
    );

    const NUM_NODES: u32 = 5;
    let (mut tx_vec, mut rx_vec) =(Vec::new(), Vec::new());
    for _ in 0..NUM_NODES {
        let (tx, rx) = mpsc::channel();
        tx_vec.push(tx);
        rx_vec.push(rx);
    }

    let (tx_stop, rx_stop) = mpsc::channel();
    let rx_stop = Arc::new(Mutex::new(rx_stop));
    
    let proposals = Arc::new(Mutex::new(VecDeque::<Proposal>::new()));
    
    let mut handles = Vec::new();
    
    for (i, rx) in rx_vec.into_iter().enumerate() {
       let mailboxes = (1..6u64).zip(tx_vec.iter().cloned()).collect();
       let mut node = match i {
           0 => Node::create_raft_leader(1, rx, mailboxes, &logger),
           _ => Node::create_raft_follower(rx, mailboxes)
       };
       let proposals = Arc::clone(&proposals);

       let mut t = Instant::now();
       
       let rx_stop_clone = Arc::clone(&rx_stop);
       let logger = logger.clone();
       
       let handle = thread::spawn(move || loop {
           thread::sleep(Duration::from_millis(10));
           loop {
               match node.my_mailbox.try_recv() {
                   Ok(msg) => node.step(msg, &logger),
                   Err(TryRecvError::Empty) => break,
                   Err(TryRecvError::Disconnected) => return,
               }
           }
           
           let raft_group = match node.raft_group {
               Some(ref mut r) => r,
               _ => continue,
           };
           
           if t.elapsed() >= Duration::from_millis(100) {
               raft_group.tick();
               t = Instant::now();
           }
           
           if raft_group.raft.state == StateRole::Leader {
               let mut proposals = proposals.lock().unwrap();
               for p in proposals.iter_mut().skip_while(|p| p.proposed > 0) {
                   info!(logger, "[raft {}] leader propose message", raft_group.raft.id);
                   propose(raft_group, p);
               }
           }
           on_ready(raft_group, &mut node.kv_pairs, &node.mailboxes, &proposals, &logger);

           if check_signals(&rx_stop_clone) {
               return;
           }
       });
       handles.push(handle);
    }
    info!(logger, "raft cluster starts now, now propose 2 proposals");

    // important
    add_all_followers(proposals.as_ref(), &logger);

    (0..2u16)
        .filter(|i| {
            let (proposal, rx) = Proposal::normal(*i, "hello world".to_owned());
            proposals.lock().unwrap().push_back(proposal);
            rx.recv().unwrap()
        })
        .count();

    for _ in 0..NUM_NODES {
        tx_stop.send(Signal::Terminate).unwrap();
    }

    for th in handles {
        th.join().unwrap();
    }
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
        // check_quorum: true,
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

    fn initialize_raft_from_message(&mut self, msg: &Message, logger: &slog::Logger) {
        info!(logger, "initialliz_raft_from_message {}", msg.to);
        if !is_initial_msg(msg) {
            return;
        }
        let mut cfg = example_config();
        cfg.id = msg.to;
        let logger = logger.new(o!("tag" => format!("peer_{}", msg.to)));
        let storage = MemStorage::new();
        self.raft_group = Some(RawNode::new(&cfg, storage).unwrap().with_logger(&logger));
    }

    fn step(&mut self, msg: Message, logger: &slog::Logger) {
        if self.raft_group.is_none() {
            if is_initial_msg(&msg) {
                self.initialize_raft_from_message(&msg, logger);
            } else {
                return;
            }
        }
        let raft_group = self.raft_group.as_mut().unwrap();
        let _ = raft_group.step(msg);
    }
}

fn is_initial_msg(msg: &Message) -> bool {
    let msg_type = msg.get_msg_type();
    msg_type == MessageType::MsgRequestVote
        || msg_type == MessageType::MsgRequestPreVote
        || (msg_type == MessageType::MsgHeartbeat && msg.commit == 0)

}

struct Proposal {
    normal: Option<(u16, String)>,
    conf_change: Option<ConfChange>,
    transfer_leader: Option<u64>,
    proposed: u64,
    propose_success: SyncSender<bool>,
}

impl Proposal {
    fn conf_change(cc: &ConfChange) -> (Self, Receiver<bool>) {
        let (tx, rx) = mpsc::sync_channel(1);
        let proposal = Proposal {
            normal: None,
            conf_change: Some(cc.clone()),
            transfer_leader: None,
            proposed: 0,
            propose_success: tx,
        };
        (proposal, rx)
    }

    fn normal(key: u16, value: String) -> (Self, Receiver<bool>) {
        let (tx, rx) = mpsc::sync_channel(1);
        let proposal = Proposal {
            normal: Some((key, value)),
            conf_change: None,
            transfer_leader: None,
            proposed: 0,
            propose_success: tx,
        };
        (proposal, rx)
    }
}

fn propose(raft_group: &mut RawNode<MemStorage>, proposal: &mut Proposal) {
   let last_index1 = raft_group.raft.raft_log.last_index() + 1;
   if let Some((ref key, ref value)) = proposal.normal {
       let data = format!("put {} {}", key, value).into_bytes();
       let _ = raft_group.propose(vec![], data);
   } else if let Some(ref cc) = proposal.conf_change {
       let _ = raft_group.propose_conf_change(vec![], cc.clone());
   }

   let last_index2 = raft_group.raft.raft_log.last_index() + 1;
   if last_index2 == last_index1 {
       proposal.propose_success.send(false).unwrap();
   } else {
       proposal.proposed = last_index1;
   }
}

fn on_ready(
    raft_group: &mut RawNode<MemStorage>,
    kv_pairs: &mut HashMap<u16, String>,
    mailboxes: &HashMap<u64, Sender<Message>>,
    proposals: &Mutex<VecDeque<Proposal>>,
    logger: &slog::Logger
) {
    let node_id = raft_group.raft.id;
    if !raft_group.has_ready() {
        return;
    }

    let store = raft_group.raft.raft_log.store.clone();

    let mut ready = raft_group.ready();

    // 持久化entries
    if let Err(e) = store.wl().append(ready.entries()) {
        error!(logger, "[raft {}] persist raft log fail: ${:?}", node_id, e);
        return;
    }

    info!(logger, "[raft {}], entries len {}", node_id, ready.entries().len());

    for msg in ready.messages.drain(..) {
        let to = msg.to;
        // send msg to other node
        if mailboxes[&to].send(msg).is_err() {
            error!(logger, "[raft {}] send raft message to [raft {}] fail", node_id, to);
        }
    }

    if let Some(committed_entries) = ready.committed_entries.take() {
        for entry in &committed_entries {
            if entry.data.is_empty() {
                continue;
            }
            if let EntryType::EntryConfChange = entry.get_entry_type() {
                let mut cc = ConfChange::default();
                cc.merge_from_bytes(&entry.data).unwrap();
                let node_id = cc.node_id;
                info!(logger, "[raft {}] receive addnode {}", raft_group.raft.id, node_id);
                match cc.get_change_type() {
                    ConfChangeType::AddNode => raft_group.raft.add_node(node_id).unwrap(),
                    _ => info!(logger, "get unexpected confchange type"),
                }
                let cs = ConfState::from(raft_group.raft.prs().configuration().clone());
                store.wl().set_conf_state(cs, None);
            }
            if raft_group.raft.state == StateRole::Leader {
                let proposal = match proposals.lock().unwrap().pop_front() {
                    Some(p) => p,
                    None => {
                        info!(logger, "[raft {}] receive empty proposal", raft_group.raft.id);
                        continue;
                    }
                };
                proposal.propose_success.send(true).unwrap();
            }
    
        }
        if let Some(last_committed) = committed_entries.last() {
            let mut s = store.wl();
            s.mut_hard_state().commit = last_committed.index;
            s.mut_hard_state().term = last_committed.term;
        }
    }
    raft_group.advance(ready);
}

fn check_signals(receiver: &Arc<Mutex<mpsc::Receiver<Signal>>>) -> bool {
    match receiver.lock().unwrap().try_recv() {
        Ok(Signal::Terminate) => true,
        Err(TryRecvError::Empty) => false,
        Err(TryRecvError::Disconnected) => true,
    }
}

fn add_all_followers(proposals: &Mutex<VecDeque<Proposal>>, logger: &slog::Logger) {
    for i in 2..6u64 {
        let mut conf_change = ConfChange::default();
        conf_change.node_id = i;
        conf_change.set_change_type(ConfChangeType::AddNode);
        loop {
            let (proposal, rx) = Proposal::conf_change(&conf_change);
            proposals.lock().unwrap().push_back(proposal);
            info!(logger, "[raft {}] make confchange", i);
            if rx.recv().unwrap() {
                break;
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
}