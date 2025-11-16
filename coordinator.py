"""
2PC Protocol - Coordinator
"""
import socket
import threading
import time
import uuid
from typing import Dict, Set
from protocol import Message, MessageType


class Coordinator:
    """协调者类"""
    
    def __init__(self, host: str = 'localhost', port: int = 5000):
        self.host = host
        self.port = port
        self.participants: Dict[str, tuple] = {}  # {participant_id: (host, port)}
        self.transactions: Dict[str, dict] = {}  # Transaction status tracking
        self.transaction_history = []  # Historical Log (in chronological order)
        self.crashed = False  # "crash status flag"
        self.lock = threading.Lock()
        self.running = False
        self.server_socket = None
        
    def start(self):
        """Start the coordinator server"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.running = True
        
        print(f"✓ The coordinator is initiated {self.host}:{self.port}")
        print("=" * 60)
        
        # Start the listening thread
        listen_thread = threading.Thread(target=self._listen_for_participants)
        listen_thread.daemon = True
        listen_thread.start()
        
        # Command-line interface
        self._command_interface()
    
    def _listen_for_participants(self):
        """Listen for the registration requests of the participants"""
        while self.running:
            try:
                self.server_socket.settimeout(1.0)
                client_socket, addr = self.server_socket.accept()
                threading.Thread(
                    target=self._handle_participant_connection, # The function to be executed by the thread
                    args=(client_socket, addr), # The positional parameters (tuples) passed to the function can also be in the form of kwargs, that is, key-value.
                    daemon=True # The daemon thread ends immediately when the main thread ends, and the child thread ends immediately
                ).start()
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    print(f"Monitoring error: {e}")
    
    def _handle_participant_connection(self, client_socket, addr):
        """Handle participant connections"""
        try:
            data = client_socket.recv(65536).decode('utf-8')
            if not data:
                return
            
            parts = data.split('|')
            request_type = parts[0]
            
            # If the coordinator has crashed, most requests will not be processed (only registered and historical requests are allowed for recovery)
            if self.crashed and request_type not in ['REGISTER', 'HISTORY_REQUEST']:
                print(f"💥 The coordinator has collapsed and refused to handle it {request_type}")
                return
            
            if request_type == 'REGISTER' and len(parts) >= 4:
                # Registration request format: REGISTER: participant_id: host: port
                participant_id = parts[1]
                participant_host = parts[2]
                participant_port = int(parts[3])
                
                with self.lock:
                    self.participants[participant_id] = (participant_host, participant_port)
                
                print(f"✓ Participants have registered: {participant_id} ({participant_host}:{participant_port})")
                client_socket.sendall(b"OK")
                
            elif request_type == 'VOTE_RESPONSE' and len(parts) >= 3:
                # 延迟投票响应格式: VOTE_RESPONSE|participant_id|{message_json}
                participant_id = parts[1]
                message_json = '|'.join(parts[2:])  # 重新组合JSON部分（可能包含|字符）
                message = Message.from_json(message_json)
                
                print(f"← Received a delayed vote: {participant_id} - {message.msg_type.value} (transaction {message.transaction_id})")
                
                # 更新事务投票状态
                with self.lock:
                    if message.transaction_id in self.transactions:
                        tx = self.transactions[message.transaction_id]
                        if message.msg_type == MessageType.VOTE_YES:
                            tx['votes'][participant_id] = True
                        else:
                            tx['votes'][participant_id] = False
            
            elif request_type == 'ACK_RESPONSE' and len(parts) >= 3:
                # 延迟ACK响应格式: ACK_RESPONSE|participant_id|{message_json}
                participant_id = parts[1]
                message_json = '|'.join(parts[2:])
                message = Message.from_json(message_json)
                
                print(f"← Receive a delayed ACK: {participant_id} - {message.msg_type.value} (transaction {message.transaction_id})")
                
                # 更新事务ACK状态
                with self.lock:
                    if message.transaction_id in self.transactions:
                        tx = self.transactions[message.transaction_id]
                        if 'acks' not in tx:
                            tx['acks'] = {}
                        tx['acks'][participant_id] = message.msg_type.value
                
            elif request_type == 'HISTORY_REQUEST' and len(parts) >= 2:
                # 历史请求格式: HISTORY_REQUEST|participant_id|{message_json}
                participant_id = parts[1]
                print(f"← Receive a historical request: {participant_id}")
                
                # 发送历史日志
                with self.lock:
                    history_data = list(self.transaction_history)
                
                response = Message(
                    MessageType.HISTORY_RESPONSE,
                    "HISTORY",
                    {"history": history_data}
                )
                client_socket.sendall(response.to_json().encode('utf-8'))
                print(f"→ Send {len(history_data)} historical transactions to {participant_id}")
                
        except Exception as e:
            print(f"Handle the participant connection error: {e}")
        finally:
            client_socket.close()
    
    def _send_message(self, participant_id: str, message: Message, force: bool = False) -> Message:
        """Send a message to the participants and wait for a response
        Args:
            participant_id: Participant ID
            message: The message to be sent
            force: Whether to force send (in recover, even crashed can be sent)
        """
        if participant_id not in self.participants:
            raise Exception(f"Participant {participant_id} does not exist.")
        
        # 如果crashed且不是强制发送，拒绝发送
        if self.crashed and not force:
            print(f"💥 The coordinator has crashed and is unable to send messages to {participant_id}")
            return None
        
        host, port = self.participants[participant_id]
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((host, port))
            sock.sendall(message.to_json().encode('utf-8'))
            
            response_data = sock.recv(4096).decode('utf-8')
            sock.close()
            
            if response_data:
                return Message.from_json(response_data)
            return None
        except Exception as e:
            print(f"Send a message to {participant_id} error: {e}")
            return None
    
    def execute_transaction(self, transaction_data: dict):
        """执行2PC事务"""
        if self.crashed:
            print("❌ The coordinator has collapsed and is unable to initiate new transactions!")
            return False
            
        transaction_id = str(uuid.uuid4())[:8]
        
        print(f"\n{'='*60}")
        print(f"Start a new business: {transaction_id}")
        print(f"Transaction data: {transaction_data}")
        print(f"The number of participants: {len(self.participants)}")
        print(f"{'='*60}")
        
        if not self.participants:
            print("❌ There are no available participants!")
            return False
        
        # 初始化事务状态
        self.transactions[transaction_id] = {
            'data': transaction_data,
            'participants': list(self.participants.keys()),
            'votes': {},
            'acks': {},
            'status': 'PREPARING'
        }
        
        # ============ 阶段1: 准备阶段 ============
        print(f"\n[Phase 1/2] Preparation Phase (PREPARE)")
        print("-" * 60)
        
        prepare_msg = Message(MessageType.PREPARE, transaction_id, transaction_data)
        votes = {}
        participant_list = list(self.participants.keys())
        
        # 发送PREPARE请求（参与者会手动投票，不会立即响应）
        for participant_id in participant_list:
            print(f"→ Send PREPARE to {participant_id}...", end=" ")
            response = self._send_message(participant_id, prepare_msg)
            
            # 有些参与者可能会立即响应（如果设置了失败率）
            if response:
                if response.msg_type == MessageType.VOTE_YES:
                    votes[participant_id] = True
                    print("✓ VOTE_YES (Immediately)")
                else:
                    votes[participant_id] = False
                    print(f"✗ {response.msg_type.value} (Immediately)")
            else:
                # 没有立即响应，等待手动投票
                print("⏳ Wait for manual voting...")
        
        self.transactions[transaction_id]['votes'] = votes
        
        # 等待所有参与者投票（最多等待60秒）
        print(f"\n⏳ Wait for all participants to vote...")
        wait_time = 0
        max_wait = 60
        while wait_time < max_wait:
            # 检查是否crash
            if self.crashed:
                print(f"\n💥 Coordinator crashes！Transaction {transaction_id} Interrupt in Phase 1")
                print(f"  Participants are in a waiting state...")
                return False
            
            with self.lock:
                current_votes = self.transactions[transaction_id]['votes']
                if len(current_votes) == len(participant_list):
                    break
            
            time.sleep(1)
            wait_time += 1
            
            # 每5秒显示一次进度
            if wait_time % 5 == 0:
                with self.lock:
                    current_votes = self.transactions[transaction_id]['votes']
                print(f"  Receive {len(current_votes)}/{len(participant_list)} votes ({wait_time}s)")
        
        # 获取最终投票结果
        with self.lock:
            votes = self.transactions[transaction_id]['votes']
        
        # 对于超时未投票的参与者，视为投NO
        for participant_id in participant_list:
            if participant_id not in votes:
                votes[participant_id] = False
                print(f"✗ {participant_id} voting exceeds the time limit and is regarded as NO")
        
        self.transactions[transaction_id]['votes'] = votes
        
        # 决定是否提交
        all_yes = all(votes.values())
        
        print(f"\nVote result: {sum(votes.values())}/{len(votes)} agreement")
        
        # ============ 阶段2: 提交/中止阶段 ============
        # 检查是否在阶段1和阶段2之间crash
        if self.crashed:
            print(f"\n💥 The coordinator broke down after making a decision! The status of the transaction {transaction_id} is uncertain")
            print(f"  Participants may be in a prepared state...")
            return False
            
        if all_yes:
            print(f"\n[Phase 2/2] COMMIT Phase")
            print("-" * 60)
            self.transactions[transaction_id]['status'] = 'COMMITTING'
            
            commit_msg = Message(MessageType.COMMIT, transaction_id, transaction_data)
            acks = {}
            
            # 发送COMMIT消息（参与者会手动ACK，不会立即响应）
            for participant_id in self.participants.keys():
                # 发送前检查是否crash
                if self.crashed:
                    print(f"\n💥 The coordinator has collapsed! Some participants did not receive the COMMIT")
                    return False
                    
                print(f"→ Send COMMIT to {participant_id}...", end=" ")
                response = self._send_message(participant_id, commit_msg)
                
                # 有些参与者可能会立即响应
                if response and response.msg_type == MessageType.ACK_COMMIT:
                    acks[participant_id] = 'ACK_COMMIT'
                    print("✓ ACK_COMMIT (Immediately)")
                else:
                    print("⏳ Wait for manual ACK...")
            
            self.transactions[transaction_id]['acks'] = acks
            
            # 等待所有参与者ACK（最多等待60秒）
            print(f"\n⏳ Waiting for all participants to ACK...")
            wait_time = 0
            max_wait = 60
            while wait_time < max_wait:
                # 检查是否crash
                if self.crashed:
                    print(f"\n💥 The coordinator crashed while waiting for ACK!")
                    return False
                
                with self.lock:
                    current_acks = self.transactions[transaction_id]['acks']
                    if len(current_acks) == len(participant_list):
                        break
                
                time.sleep(1)
                wait_time += 1
                
                # 每5秒显示一次进度
                if wait_time % 5 == 0:
                    with self.lock:
                        current_acks = self.transactions[transaction_id]['acks']
                    print(f"  Receive {len(current_acks)}/{len(participant_list)} ACK ({wait_time}s)")
            
            # 获取最终ACK结果
            with self.lock:
                acks = self.transactions[transaction_id]['acks']
            
            # 对于超时未ACK的参与者，标记为超时
            for participant_id in participant_list:
                if participant_id not in acks:
                    acks[participant_id] = 'TIMEOUT'
                    print(f"✗ {participant_id} ACK timeout")
            
            self.transactions[transaction_id]['acks'] = acks
            success_count = sum(1 for ack in acks.values() if ack == 'ACK_COMMIT')
            
            self.transactions[transaction_id]['status'] = 'COMMITTED'
            
            # 记录到历史日志
            with self.lock:
                self.transaction_history.append({
                    'transaction_id': transaction_id,
                    'status': 'COMMITTED',
                    'data': transaction_data,
                    'timestamp': time.time()
                })
            
            print(f"\n{'='*60}")
            print(f"✓ Transaction {transaction_id} submit successfully! ({success_count}/{len(self.participants)} confirm)")
            print(f"{'='*60}")
            return True
        else:
            print(f"\n[Phase 2/2] ABORT Phase")
            print("-" * 60)
            self.transactions[transaction_id]['status'] = 'ABORTING'
            
            abort_msg = Message(MessageType.ABORT, transaction_id, transaction_data)
            acks = {}
            
            # 发送ABORT消息（参与者会手动ACK，不会立即响应）
            for participant_id in self.participants.keys():
                # 发送前检查是否crash
                if self.crashed:
                    print(f"\n💥 The coordinator has collapsed! Some participants did not receive the ABORT")
                    return False
                    
                print(f"→ Send ABORT to {participant_id}...", end=" ")
                response = self._send_message(participant_id, abort_msg)
                
                # 有些参与者可能会立即响应
                if response and response.msg_type == MessageType.ACK_ABORT:
                    acks[participant_id] = 'ACK_ABORT'
                    print("✓ ACK_ABORT (Immediately)")
                else:
                    print("⏳ Wait for manual ACK...")
            
            self.transactions[transaction_id]['acks'] = acks
            
            # 等待所有参与者ACK（最多等待60秒）
            print(f"\n⏳ Waiting for all participants to ACK...")
            wait_time = 0
            max_wait = 60
            while wait_time < max_wait:
                # 检查是否crash
                if self.crashed:
                    print(f"\n💥 The coordinator crashed while waiting for ACK!")
                    return False
                
                with self.lock:
                    current_acks = self.transactions[transaction_id]['acks']
                    if len(current_acks) == len(participant_list):
                        break
                
                time.sleep(1)
                wait_time += 1
                
                # 每5秒显示一次进度
                if wait_time % 5 == 0:
                    with self.lock:
                        current_acks = self.transactions[transaction_id]['acks']
                    print(f" Receive {len(current_acks)}/{len(participant_list)} ACK ({wait_time}s)")
            
            # 获取最终ACK结果
            with self.lock:
                acks = self.transactions[transaction_id]['acks']
            
            # 对于超时未ACK的参与者，标记为超时
            for participant_id in participant_list:
                if participant_id not in acks:
                    acks[participant_id] = 'TIMEOUT'
                    print(f"✗ {participant_id} ACK timeout")
            
            self.transactions[transaction_id]['acks'] = acks
            success_count = sum(1 for ack in acks.values() if ack == 'ACK_ABORT')
            
            self.transactions[transaction_id]['status'] = 'ABORTED'
            
            # 记录到历史日志
            with self.lock:
                self.transaction_history.append({
                    'transaction_id': transaction_id,
                    'status': 'ABORTED',
                    'data': transaction_data,
                    'timestamp': time.time()
                })
            
            print(f"\n{'='*60}")
            print(f"✗ Transaction {transaction_id} is aborted")
            print(f"{'='*60}")
            return False
    
    def _query_participant_state(self, participant_id: str, transaction_id: str) -> dict:
        """查询参与者对特定事务的状态"""
        try:
            query_msg = Message(MessageType.QUERY_STATE, transaction_id, {})
            # recover时需要强制发送消息
            response = self._send_message(participant_id, query_msg, force=True)
            
            if response and response.msg_type == MessageType.STATE_RESPONSE:
                return response.data
            return {'status': 'UNKNOWN'}
        except Exception as e:
            print(f"  Query {participant_id} status fails: {e}")
            return {'status': 'UNKNOWN'}
    
    def _recover_coordinator(self):
        """协调者从崩溃中恢复"""
        print(f"\n🔄 Start Coordinator recovery...")
        print("=" * 60)
        
        # 查找未完成的事务
        with self.lock:
            unfinished_txs = {
                tx_id: tx_info 
                for tx_id, tx_info in self.transactions.items()
                if tx_info['status'] in ['PREPARING', 'COMMITTING', 'ABORTING']
            }
        
        if not unfinished_txs:
            print("✓ There are no unfinished tasks.")
            self.crashed = False
            return
        
        print(f" Find {len(unfinished_txs)} unfinished transaction.")
        print()
        
        for tx_id, tx_info in unfinished_txs.items():
            print(f"\nHandle transactions {tx_id}:")
            print(f"  Status: {tx_info['status']}")
            print(f"  Data: {tx_info['data']}")
            
            # 查询所有参与者的状态
            print(f"  Query the status of participants...")
            participant_states = {}
            for participant_id in tx_info['participants']:
                if participant_id not in self.participants:
                    print(f"    {participant_id}: Unregistered")
                    continue
                
                state = self._query_participant_state(participant_id, tx_id)
                participant_states[participant_id] = state
                print(f"    {participant_id}: {state.get('status', 'UNKNOWN')}")
            
            # 根据状态决定如何处理
            prepared_count = sum(1 for s in participant_states.values() 
                               if s.get('status') == 'PREPARED')
            committed_count = sum(1 for s in participant_states.values() 
                                if s.get('status') == 'COMMITTED')
            aborted_count = sum(1 for s in participant_states.values() 
                              if s.get('status') == 'ABORTED')
            
            print(f"\n  Status summary.")
            print(f"    PREPARED: {prepared_count}")
            print(f"    COMMITTED: {committed_count}")
            print(f"    ABORTED: {aborted_count}")
            
            # 决策逻辑
            if tx_info['status'] == 'PREPARING':
                # 在准备阶段crash，检查投票情况
                votes = tx_info.get('votes', {})
                if len(votes) == len(tx_info['participants']) and all(votes.values()):
                    # 所有人都投了YES，但还没发COMMIT，现在发送COMMIT
                    print(f"  💡 Decision: All participants are ready to send the COMMIT")
                    self._complete_commit(tx_id, tx_info)
                else:
                    # 投票未完成或有NO，发送ABORT
                    print(f"  💡 Decision: If the voting is not completed or there is a rejection, send "ABORT"")
                    self._complete_abort(tx_id, tx_info)
                    
            elif tx_info['status'] == 'COMMITTING':
                # 在提交阶段crash
                if committed_count > 0:
                    # 有参与者已提交，继续COMMIT
                    print(f"  💡 Decision: Some participants have already submitted. Continue to send commits")
                    self._complete_commit(tx_id, tx_info)
                elif prepared_count == len(tx_info['participants']):
                    # 所有参与者都在prepared状态，继续COMMIT
                    print(f"  💡 Decision: All participants are ready to continue sending commits")
                    self._complete_commit(tx_id, tx_info)
                else:
                    # 状态不一致，尝试COMMIT
                    print(f"  💡 Decision: Attempt to complete the COMMIT")
                    self._complete_commit(tx_id, tx_info)
                    
            elif tx_info['status'] == 'ABORTING':
                # 在中止阶段crash，继续ABORT
                print(f"  💡 Decision: Continue sending ABORT")
                self._complete_abort(tx_id, tx_info)
        
        self.crashed = False
        print(f"\n{'='*60}")
        print("✓ The coordinator's recovery is complete!")
        print(f"{'='*60}")
    
    def _complete_commit(self, transaction_id: str, tx_info: dict):
        """完成提交操作"""
        commit_msg = Message(MessageType.COMMIT, transaction_id, tx_info['data'])
        success_count = 0
        
        for participant_id in tx_info['participants']:
            if participant_id not in self.participants:
                continue
            print(f"    → Send COMMIT to {participant_id}...", end=" ")
            # recover时需要强制发送消息
            response = self._send_message(participant_id, commit_msg, force=True)
            if response and response.msg_type == MessageType.ACK_COMMIT:
                success_count += 1
                print("✓")
            else:
                print("✗")
        
        with self.lock:
            self.transactions[transaction_id]['status'] = 'COMMITTED'
            self.transaction_history.append({
                'transaction_id': transaction_id,
                'status': 'COMMITTED',
                'data': tx_info['data'],
                'timestamp': time.time()
            })
        print(f"    ✓ The transaction has been committed. ({success_count}/{len(tx_info['participants'])})")
    
    def _complete_abort(self, transaction_id: str, tx_info: dict):
        """完成中止操作"""
        abort_msg = Message(MessageType.ABORT, transaction_id, tx_info['data'])
        success_count = 0
        
        for participant_id in tx_info['participants']:
            if participant_id not in self.participants:
                continue
            print(f"    → Send ABORT to {participant_id}...", end=" ")
            # recover时需要强制发送消息
            response = self._send_message(participant_id, abort_msg, force=True)
            if response and response.msg_type == MessageType.ACK_ABORT:
                success_count += 1
                print("✓")
            else:
                print("✗")
        
        with self.lock:
            self.transactions[transaction_id]['status'] = 'ABORTED'
            self.transaction_history.append({
                'transaction_id': transaction_id,
                'status': 'ABORTED',
                'data': tx_info['data'],
                'timestamp': time.time()
            })
        print(f"    ✓ The transaction has been suspended. ({success_count}/{len(tx_info['participants'])})")
    
    def _command_interface(self):
        """命令行界面"""
        # print("\n可用命令:")
        # print("  list    - 列出所有参与者")
        # print("  tx      - 发起新事务")
        # print("  crash   - 模拟崩溃")
        # print("  recover - 从崩溃中恢复")
        # print("  status  - 查看事务状态")
        # print("  quit    - 退出")
        # print()
        print(\n available command:)
        print(" list - List all participants ")
        print(" tx - Initiate a new transaction ")
        print(" crash - Simulated crash ")
        print(" recover - Recover from crash ")
        print(" status - View transaction status ")
        print(" quit ")
        print()
        
        while self.running:
            try:
                status_prefix = "💥CRASHED" if self.crashed else "coordinator"
                cmd = input(f"{status_prefix}> ").strip().lower()
                
                if cmd == 'quit':
                    self.stop()
                    break
                elif cmd == 'list':
                    self._list_participants()
                elif cmd == 'tx':
                    self._start_transaction()
                elif cmd == 'crash':
                    self._handle_crash()
                elif cmd == 'recover':
                    self._handle_recover()
                elif cmd == 'status':
                    self._show_status()
                else:
                    print("Unknown command, please use: list, tx, crash, recover, status, quit")
            except KeyboardInterrupt:
                print("\n Use the 'quit' command to exit")
            except Exception as e:
                print(f"Error: {e}")
    
    def _handle_crash(self):
        """处理崩溃命令"""
        if self.crashed:
            print("It is already in a state of collapse.")
            return
        
        self.crashed = True
        print(f"\n💥 coordinator has crashed!" )
        print(" - Cannot initiate a new transaction ")
        print(" - Unfinished transactions will be suspended ")
        print(" - Participants may be in a waiting state ")
        print(" - Restore using the 'recover' command ")
    
    def _handle_recover(self):
        """处理恢复命令"""
        if not self.crashed:
            print("It is not currently in a state of collapse")
            return
        
        self._recover_coordinator()
    
    def _list_participants(self):
        """列出所有参与者"""
        print(f"\n Registered participants ({len(self.participants)}):")
        if self.participants:
            for pid, (host, port) in self.participants.items():
                print(f"  - {pid} ({host}:{port})")
        else:
            print("  (无)")
    
    def _start_transaction(self):
        """发起新事务"""
        print("\nPlease enter the transaction data (Format: key=value, Example: account=alice,amount=100):")
        data_str = input("data> ").strip()
        
        if not data_str:
            print("Transaction data cannot be empty")
            return
        
        # 解析数据
        transaction_data = {}
        for pair in data_str.split(','):
            if '=' in pair:
                key, value = pair.split('=', 1)
                transaction_data[key.strip()] = value.strip()
        
        if transaction_data:
            # 在后台线程执行事务，这样命令行界面可以继续接收命令（如crash）
            tx_thread = threading.Thread(
                target=self.execute_transaction,
                args=(transaction_data,),
                daemon=True
            )
            tx_thread.start()
            print("✓ The transaction has been started in the background. You can enter the 'crash' command at any time to simulate a crash")
        else:
            print("Invalid data format")
    
    def _show_status(self):
        """显示事务状态"""
        print(f"\n Transaction history ({len(self.transactions)}):")
        if self.transactions:
            for tx_id, tx_info in self.transactions.items():
                print(f"  {tx_id}: {tx_info['status']} - {tx_info['data']}")
        else:
            print("  (Empty)")
    
    def stop(self):
        """停止协调者"""
        print("\nThe coordinator is being shut down...")
        self.running = False
        if self.server_socket:
            self.server_socket.close()


def main():
    import sys
    
    port = 5000
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    
    coordinator = Coordinator(port=port)
    try:
        coordinator.start()
    except KeyboardInterrupt:
        coordinator.stop()


if __name__ == '__main__':
    main()

