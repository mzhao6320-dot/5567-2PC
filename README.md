# 2PC (Two-Phase Commit Protocol) Demonstration Program

This is a demonstration system of the Two-Phase Commit Protocol implemented with Python 3.8. This system consists of a Coordinator and multiple participants, who achieve consensus on distributed transactions through network communication.

## 📋 Functional features

- ✅ complete 2PC protocol implementation (preparation phase + commit/abort phase)
- ✅ The coordinator and participants are separated and can be initiated independently
- ✅ network communication based on TCP Socket
- ✅ supports multiple participants running simultaneously
- ✅ interactive command-line interface
- ✅ configurable failure rate simulation
- ✅ real-time transaction status tracking
- 🆕 ** manual voting mechanism ** - participants manually decide to vote YES or NO
- 🆕 **Crash feature ** - simulate participant crash
- 🆕 **Recover feature ** - recover from crashes and synchronize history logs

## 🏗️ system architecture

` ` `
┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
"Coordinator"
│   Port: 5000    │
└ ─ ─ ─ ─ ─ ─ ─ ─ ┬ ─ ─ ─ ─ ─ ─ ─ ─ ┘
│
┌ ─ ─ ─ ─ ┴ ─ ─ ─ ─ ┬ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
│ │ │
───▼──┐ ──▼──┐ ──▼──┐
│ P1 │ │ P2 │ │ P3 │ (Participants
│ 6001 │ │ 6002 │ │ 6003 │
└──────┘ └───── ┘ └───── ┘
` ` `

## 🚀 get started quickly

1. Start the coordinator

Run in the first terminal window:

```bash
python coordinator.py
` ` `

The default port is 5000. If other ports need to be specified:

```bash
python coordinator.py 5000
` ` `

2. Start the participants

Start multiple participants separately in other terminal Windows:

Terminal 2
```bash
python participant.py P1 6001
` ` `

Terminal 3
```bash
python participant.py P2 6002
` ` `

Terminal 4
```bash
python participant.py P3 6003
` ` `

Parameter description
- 'P1', 'P2', 'P3' : Participant ID (customizable)
- '6001', '6002', '6003' : Participant port numbers
- If you need to specify the coordinator port: 'python participant.py P1 6001 5000'

3. Execute transactions

In the Coordinator terminal:

` ` `
coordinator> tx
Please enter the transaction data (format: key=value, example: account=alice,amount=100):
data> account=alice,amount=100,operation=deposit
` ` `

The system will automatically execute the 2PC protocol:
1. ** Phase 1 (Preparation) **: The coordinator sends a PREPARE request to all participants
2. ** Voting **: Participants respond with YES or NO
3. ** Phase 2 (Submit/Abort) **:
If all participants vote "YES", the coordinator sends a COMMIT
If any participant votes NO, the coordinator sends an ABORT

## 📖 command description

Coordinator's Order

Command: Explanation
|------|------|
'list' : List all registered participants
'tx' initiate a new transaction
'status' view transaction history and status
'quit' means to exit the coordinator

Participant Command

Command: Explanation
|------|------|
'status' : View the status of participants
"data" : View the submitted transaction data
'vote yes/no' : 🆕 vote on the voting transaction
'crash' : 🆕 simulate a crash
'recover' : 🆕 recover from a crash and synchronize history
Set the failure rate (simulating network failure)
{" id ": 1313841," text ":" > 'quit' > ">" Exit participant >

## 🔬 protocol process example

` ` `
[Coordinator] Initiates transaction TX-001
[Coordinator] -- PREPARE -- > [P1, P2, P3]
[P1] < Ready to Succeed > -- VOTE_YES -- > [Coordinator]
[P2] < Ready to Succeed > -- VOTE_YES -- > [Coordinator]
[P3] < Ready to Succeed > -- VOTE_YES -- > [Coordinator]
[Coordinator] All votes passed and it was decided to submit
[Coordinator] -- COMMIT -- > [P1, P2, P3]
[P1] < Commit Successful > -- ACK_COMMIT -- > [Coordinator]
[P2] < Commit Successful > -- ACK_COMMIT -- > [Coordinator]
[P3] < Commit Successful > -- ACK_COMMIT -- > [Coordinator]
[Coordinator] Transaction successfully committed ✓
` ` `

## 🧪 test scenario

Scene 1: Normal submission

All participants are working properly and the transaction should be successfully submitted.

Scene 2: Participants refuse

Set the failure rate among a certain participant:
` ` `
P2> fail
Input failure rate (0.0-1.0): 1.0
` ` `

After initiating a transaction, P2 will vote VOTE_NO, causing the entire transaction to abort.

Scene 3: Multiple participants

Start four or more participants to test whether the coordinator can manage multiple nodes correctly.

Scene 4: Dynamic registration

During the coordinator's operation, new participants can be started to join the network at any time.

## 📁 file structure

` ` `
2pc/
├── Coordinator.py # coordinator program
├── participant.py # participant program
├── Protocol.py # protocol message definition
├── requirements.txt # Dependencies (This project only uses the standard library)
└── README.md # This document
` ` `

## 🛠️ technical implementation

- ** Language: Python 3.8
- ** Network **: TCP Socket
- ** Concurrency **: Threading
- "Serialization" : JSON
- ** Architecture **: Client-Server mode

## 📝 protocol message type

Coordinator → Participant
- 'PREPARE' : Prepare the phase request
- 'COMMIT' : Submit the request
- 'ABORT' : Abort the request

Participant → Coordinator
- 'VOTE_YES' : Vote in favor
- 'VOTE_NO' : Vote rejected
- 'ACK_COMMIT' : Confirm the commit
- 'ACK_ABORT' : Confirm abort

## ⚠️ notes

1. This system is a demonstration program for learning and understanding the 2PC protocol and is not suitable for production environments
2. Persistent storage and fault recovery have not been implemented
3. Network partitioning and timeout recovery were not handled
4. The coordinator is a single point, and the coordinator failover has not been implemented

## 🎓 Key points to learn

Through this demonstration, you can learn:
The complete process of the 2PC protocol
Consensus issues in distributed systems
- Network programming and Socket communication
- Multi-threaded concurrent processing
- Transaction status management

## 📚 expand thinking

What would happen if the coordinator collapsed in Phase 2?
What would happen if a participant crashed after preparation but before submission?
Where is the performance bottleneck of -2PC?
How can 3PC (three-phase submission) improve 2PC?
What are the advantages of modern consensus algorithms such as Paxos and Raft?

## 🤝 contribution

Welcome to submit issues and Pull requests!

## 📄 License

MIT License
