use chrono::Utc;
use std::sync::Mutex;

// The ID as a whole is a 63 bit integer stored in an int64
// 41 bits are used to store a timestamp with millisecond precision, using a custom epoch.
// 10 bits are used to store a node/datacenter id - a range from 0 through 1023.
// 12 bits are used to store a sequence number - a range from 0 through 4095.
// +----------------------------------------------------------------------------------------------+
// | 1 Bit Unused | 41 Bit Timestamp |  5 Bit NodeID  | 5 Bit DatacenterID |   12 Bit Sequence ID |
// +----------------------------------------------------------------------------------------------+;
const SEQUENCE_BITS: i64 = 12;
const MAX_SEQUENCE: i64 = -1i64 ^ (-1i64 << SEQUENCE_BITS);
const TIME_SHIFT: i64 = 22; // TIME_SHIFT = NODE_BITS + SEQUENCE_BITS
const NODE_SHIFT: i64 = 17; // NODE_SHIFT = DATACENTER_BITS + SEQUENCE_BITS
const DATA_SHIFT: i64 = 12; // DATA_SHIFT = SEQUENCE_BITS

// Service sentinel date: 2020-05-20 08:00:00 +0800 CST
const EPOCH: i64 = 1589923200000;

#[derive(Debug)]
struct State {
    last_timestamp: i64,
    sequence: i64,
}

/// A Distributed Unique ID generator.
pub struct Generator {
    node_id: i64,
    datacenter_id: i64,
    mu: Mutex<State>,
}

impl Generator {
    pub fn new(node_id: i64, datacenter_id: i64) -> Self {
        return Generator {
            node_id,
            datacenter_id,
            mu: Mutex::new(State {
                last_timestamp: 0,
                sequence: 0,
            }),
        };
    }

    /// Each time you generate an ID:
    /// - A timestamp with millisecond precision is stored using 41 bits of the ID.
    /// - The NodeID and DatacenterIDs are added in subsequent bits.
    /// - the Sequence Number is added, starting at 0 and incrementing for each ID generated in the same millisecond.
    /// - If enough IDs are generated in the same millisecond, causing the sequence to overfill, then the function will pause until the next millisecond.
    pub fn next_id(&self) -> Result<i64, &'static str> {
        let mut state = self.mu.lock().unwrap();
        let mut now = self.get_milliseconds();

        if now < state.last_timestamp {
            return Err("it appears that time is moving backwards");
        }

        if now == state.last_timestamp {
            state.sequence = (state.sequence + 1) & MAX_SEQUENCE;
            if state.sequence == 0 {
                while now <= state.last_timestamp {
                    now = self.get_milliseconds();
                }
            }
        } else {
            state.sequence = 0;
        }

        state.last_timestamp = now;

        let id = (now - EPOCH) << TIME_SHIFT
            | self.node_id << NODE_SHIFT
            | self.datacenter_id << DATA_SHIFT
            | state.sequence;

        Ok(id)
    }

    fn get_milliseconds(&self) -> i64 {
        Utc::now().timestamp_millis()
    }
}

#[test]
fn it_works() {
    let generator = Generator::new(0, 0);
    let id1 = generator.next_id().unwrap();
    let id2 = generator.next_id().unwrap();
    println!("id1: {}, id2: {}", id1, id2);
    assert_ne!(id1, id2);
}

#[test]
fn it_really_works() {
    let generator = Generator::new(1, 1);
    let mut set = std::collections::HashSet::<i64>::new();

    for _ in 0..1_000_000 {
        let id = generator.next_id().unwrap();
        println!("id = {}", id);
        set.insert(id);
    }

    assert_eq!(set.len(), 1_000_000)
}
