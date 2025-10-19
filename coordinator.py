"""
2PC协议 - 协调者（Coordinator）
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
        self.transactions: Dict[str, dict] = {}  # 事务状态跟踪
        self.transaction_history = []  # 历史日志（按时间顺序）
        self.crashed = False  # crash状态标志
        self.lock = threading.Lock()
        self.running = False
        self.server_socket = None
        
    def start(self):
        """启动协调者服务器"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.running = True
        
        print(f"✓ 协调者启动在 {self.host}:{self.port}")
        print("=" * 60)
        
        # 启动监听线程
        listen_thread = threading.Thread(target=self._listen_for_participants)
        listen_thread.daemon = True
        listen_thread.start()
        
        # 命令行界面
        self._command_interface()
    
    def _listen_for_participants(self):
        """监听参与者的注册请求"""
        while self.running:
            try:
                self.server_socket.settimeout(1.0)
                client_socket, addr = self.server_socket.accept()
                threading.Thread(
                    target=self._handle_participant_connection, # 线程要执行的函数
                    args=(client_socket, addr), # 传递给函数的位置参数（元组）， 也可以使用kwargs，即key-value的形式，
                    daemon=True # 守护线程，主线程结束，子线程立即结束
                ).start()
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    print(f"监听错误: {e}")
    
    def _handle_participant_connection(self, client_socket, addr):
        """处理参与者连接"""
        try:
            data = client_socket.recv(65536).decode('utf-8')
            if not data:
                return
            
            parts = data.split('|')
            request_type = parts[0]
            
            # 如果协调者已崩溃，拒绝处理大部分请求（只允许注册和历史请求用于恢复）
            if self.crashed and request_type not in ['REGISTER', 'HISTORY_REQUEST']:
                print(f"💥 协调者已崩溃，拒绝处理 {request_type}")
                return
            
            if request_type == 'REGISTER' and len(parts) >= 4:
                # 注册请求格式: REGISTER|participant_id|host|port
                participant_id = parts[1]
                participant_host = parts[2]
                participant_port = int(parts[3])
                
                with self.lock:
                    self.participants[participant_id] = (participant_host, participant_port)
                
                print(f"✓ 参与者已注册: {participant_id} ({participant_host}:{participant_port})")
                client_socket.sendall(b"OK")
                
            elif request_type == 'VOTE_RESPONSE' and len(parts) >= 3:
                # 延迟投票响应格式: VOTE_RESPONSE|participant_id|{message_json}
                participant_id = parts[1]
                message_json = '|'.join(parts[2:])  # 重新组合JSON部分（可能包含|字符）
                message = Message.from_json(message_json)
                
                print(f"← 收到延迟投票: {participant_id} - {message.msg_type.value} (事务 {message.transaction_id})")
                
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
                
                print(f"← 收到延迟ACK: {participant_id} - {message.msg_type.value} (事务 {message.transaction_id})")
                
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
                print(f"← 收到历史请求: {participant_id}")
                
                # 发送历史日志
                with self.lock:
                    history_data = list(self.transaction_history)
                
                response = Message(
                    MessageType.HISTORY_RESPONSE,
                    "HISTORY",
                    {"history": history_data}
                )
                client_socket.sendall(response.to_json().encode('utf-8'))
                print(f"→ 已发送 {len(history_data)} 条历史记录给 {participant_id}")
                
        except Exception as e:
            print(f"处理参与者连接错误: {e}")
        finally:
            client_socket.close()
    
    def _send_message(self, participant_id: str, message: Message, force: bool = False) -> Message:
        """向参与者发送消息并等待响应
        
        Args:
            participant_id: 参与者ID
            message: 要发送的消息
            force: 是否强制发送（用于recover时，即使crashed也能发送）
        """
        if participant_id not in self.participants:
            raise Exception(f"参与者 {participant_id} 不存在")
        
        # 如果crashed且不是强制发送，拒绝发送
        if self.crashed and not force:
            print(f"💥 协调者已崩溃，无法发送消息到 {participant_id}")
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
            print(f"发送消息到 {participant_id} 失败: {e}")
            return None
    
    def execute_transaction(self, transaction_data: dict):
        """执行2PC事务"""
        if self.crashed:
            print("❌ 协调者已崩溃，无法发起新事务！")
            return False
            
        transaction_id = str(uuid.uuid4())[:8]
        
        print(f"\n{'='*60}")
        print(f"开始新事务: {transaction_id}")
        print(f"事务数据: {transaction_data}")
        print(f"参与者数量: {len(self.participants)}")
        print(f"{'='*60}")
        
        if not self.participants:
            print("❌ 没有可用的参与者！")
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
        print(f"\n[阶段 1/2] 准备阶段 (PREPARE)")
        print("-" * 60)
        
        prepare_msg = Message(MessageType.PREPARE, transaction_id, transaction_data)
        votes = {}
        participant_list = list(self.participants.keys())
        
        # 发送PREPARE请求（参与者会手动投票，不会立即响应）
        for participant_id in participant_list:
            print(f"→ 发送PREPARE到 {participant_id}...", end=" ")
            response = self._send_message(participant_id, prepare_msg)
            
            # 有些参与者可能会立即响应（如果设置了失败率）
            if response:
                if response.msg_type == MessageType.VOTE_YES:
                    votes[participant_id] = True
                    print("✓ VOTE_YES (立即)")
                else:
                    votes[participant_id] = False
                    print(f"✗ {response.msg_type.value} (立即)")
            else:
                # 没有立即响应，等待手动投票
                print("⏳ 等待手动投票...")
        
        self.transactions[transaction_id]['votes'] = votes
        
        # 等待所有参与者投票（最多等待60秒）
        print(f"\n⏳ 等待所有参与者投票...")
        wait_time = 0
        max_wait = 60
        while wait_time < max_wait:
            # 检查是否crash
            if self.crashed:
                print(f"\n💥 协调者崩溃！事务 {transaction_id} 在阶段1中断")
                print(f"  参与者处于等待状态...")
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
                print(f"  已收到 {len(current_votes)}/{len(participant_list)} 个投票 ({wait_time}s)")
        
        # 获取最终投票结果
        with self.lock:
            votes = self.transactions[transaction_id]['votes']
        
        # 对于超时未投票的参与者，视为投NO
        for participant_id in participant_list:
            if participant_id not in votes:
                votes[participant_id] = False
                print(f"✗ {participant_id} 投票超时，视为 NO")
        
        self.transactions[transaction_id]['votes'] = votes
        
        # 决定是否提交
        all_yes = all(votes.values())
        
        print(f"\n投票结果: {sum(votes.values())}/{len(votes)} 同意")
        
        # ============ 阶段2: 提交/中止阶段 ============
        # 检查是否在阶段1和阶段2之间crash
        if self.crashed:
            print(f"\n💥 协调者在决策后崩溃！事务 {transaction_id} 状态不确定")
            print(f"  参与者可能处于prepared状态...")
            return False
            
        if all_yes:
            print(f"\n[阶段 2/2] 提交阶段 (COMMIT)")
            print("-" * 60)
            self.transactions[transaction_id]['status'] = 'COMMITTING'
            
            commit_msg = Message(MessageType.COMMIT, transaction_id, transaction_data)
            acks = {}
            
            # 发送COMMIT消息（参与者会手动ACK，不会立即响应）
            for participant_id in self.participants.keys():
                # 发送前检查是否crash
                if self.crashed:
                    print(f"\n💥 协调者崩溃！部分参与者未收到COMMIT")
                    return False
                    
                print(f"→ 发送COMMIT到 {participant_id}...", end=" ")
                response = self._send_message(participant_id, commit_msg)
                
                # 有些参与者可能会立即响应
                if response and response.msg_type == MessageType.ACK_COMMIT:
                    acks[participant_id] = 'ACK_COMMIT'
                    print("✓ ACK_COMMIT (立即)")
                else:
                    print("⏳ 等待手动ACK...")
            
            self.transactions[transaction_id]['acks'] = acks
            
            # 等待所有参与者ACK（最多等待60秒）
            print(f"\n⏳ 等待所有参与者ACK...")
            wait_time = 0
            max_wait = 60
            while wait_time < max_wait:
                # 检查是否crash
                if self.crashed:
                    print(f"\n💥 协调者在等待ACK时崩溃！")
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
                    print(f"  已收到 {len(current_acks)}/{len(participant_list)} 个ACK ({wait_time}s)")
            
            # 获取最终ACK结果
            with self.lock:
                acks = self.transactions[transaction_id]['acks']
            
            # 对于超时未ACK的参与者，标记为超时
            for participant_id in participant_list:
                if participant_id not in acks:
                    acks[participant_id] = 'TIMEOUT'
                    print(f"✗ {participant_id} ACK超时")
            
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
            print(f"✓ 事务 {transaction_id} 提交成功! ({success_count}/{len(self.participants)} 确认)")
            print(f"{'='*60}")
            return True
        else:
            print(f"\n[阶段 2/2] 中止阶段 (ABORT)")
            print("-" * 60)
            self.transactions[transaction_id]['status'] = 'ABORTING'
            
            abort_msg = Message(MessageType.ABORT, transaction_id, transaction_data)
            acks = {}
            
            # 发送ABORT消息（参与者会手动ACK，不会立即响应）
            for participant_id in self.participants.keys():
                # 发送前检查是否crash
                if self.crashed:
                    print(f"\n💥 协调者崩溃！部分参与者未收到ABORT")
                    return False
                    
                print(f"→ 发送ABORT到 {participant_id}...", end=" ")
                response = self._send_message(participant_id, abort_msg)
                
                # 有些参与者可能会立即响应
                if response and response.msg_type == MessageType.ACK_ABORT:
                    acks[participant_id] = 'ACK_ABORT'
                    print("✓ ACK_ABORT (立即)")
                else:
                    print("⏳ 等待手动ACK...")
            
            self.transactions[transaction_id]['acks'] = acks
            
            # 等待所有参与者ACK（最多等待60秒）
            print(f"\n⏳ 等待所有参与者ACK...")
            wait_time = 0
            max_wait = 60
            while wait_time < max_wait:
                # 检查是否crash
                if self.crashed:
                    print(f"\n💥 协调者在等待ACK时崩溃！")
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
                    print(f"  已收到 {len(current_acks)}/{len(participant_list)} 个ACK ({wait_time}s)")
            
            # 获取最终ACK结果
            with self.lock:
                acks = self.transactions[transaction_id]['acks']
            
            # 对于超时未ACK的参与者，标记为超时
            for participant_id in participant_list:
                if participant_id not in acks:
                    acks[participant_id] = 'TIMEOUT'
                    print(f"✗ {participant_id} ACK超时")
            
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
            print(f"✗ 事务 {transaction_id} 已中止")
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
            print(f"  查询 {participant_id} 状态失败: {e}")
            return {'status': 'UNKNOWN'}
    
    def _recover_coordinator(self):
        """协调者从崩溃中恢复"""
        print(f"\n🔄 开始协调者恢复...")
        print("=" * 60)
        
        # 查找未完成的事务
        with self.lock:
            unfinished_txs = {
                tx_id: tx_info 
                for tx_id, tx_info in self.transactions.items()
                if tx_info['status'] in ['PREPARING', 'COMMITTING', 'ABORTING']
            }
        
        if not unfinished_txs:
            print("✓ 没有未完成的事务")
            self.crashed = False
            return
        
        print(f"发现 {len(unfinished_txs)} 个未完成的事务")
        print()
        
        for tx_id, tx_info in unfinished_txs.items():
            print(f"\n处理事务 {tx_id}:")
            print(f"  状态: {tx_info['status']}")
            print(f"  数据: {tx_info['data']}")
            
            # 查询所有参与者的状态
            print(f"  查询参与者状态...")
            participant_states = {}
            for participant_id in tx_info['participants']:
                if participant_id not in self.participants:
                    print(f"    {participant_id}: 未注册")
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
            
            print(f"\n  状态汇总:")
            print(f"    PREPARED: {prepared_count}")
            print(f"    COMMITTED: {committed_count}")
            print(f"    ABORTED: {aborted_count}")
            
            # 决策逻辑
            if tx_info['status'] == 'PREPARING':
                # 在准备阶段crash，检查投票情况
                votes = tx_info.get('votes', {})
                if len(votes) == len(tx_info['participants']) and all(votes.values()):
                    # 所有人都投了YES，但还没发COMMIT，现在发送COMMIT
                    print(f"  💡 决策: 所有参与者已准备，发送COMMIT")
                    self._complete_commit(tx_id, tx_info)
                else:
                    # 投票未完成或有NO，发送ABORT
                    print(f"  💡 决策: 投票未完成或有拒绝，发送ABORT")
                    self._complete_abort(tx_id, tx_info)
                    
            elif tx_info['status'] == 'COMMITTING':
                # 在提交阶段crash
                if committed_count > 0:
                    # 有参与者已提交，继续COMMIT
                    print(f"  💡 决策: 部分参与者已提交，继续发送COMMIT")
                    self._complete_commit(tx_id, tx_info)
                elif prepared_count == len(tx_info['participants']):
                    # 所有参与者都在prepared状态，继续COMMIT
                    print(f"  💡 决策: 所有参与者已准备，继续发送COMMIT")
                    self._complete_commit(tx_id, tx_info)
                else:
                    # 状态不一致，尝试COMMIT
                    print(f"  💡 决策: 尝试完成COMMIT")
                    self._complete_commit(tx_id, tx_info)
                    
            elif tx_info['status'] == 'ABORTING':
                # 在中止阶段crash，继续ABORT
                print(f"  💡 决策: 继续发送ABORT")
                self._complete_abort(tx_id, tx_info)
        
        self.crashed = False
        print(f"\n{'='*60}")
        print("✓ 协调者恢复完成！")
        print(f"{'='*60}")
    
    def _complete_commit(self, transaction_id: str, tx_info: dict):
        """完成提交操作"""
        commit_msg = Message(MessageType.COMMIT, transaction_id, tx_info['data'])
        success_count = 0
        
        for participant_id in tx_info['participants']:
            if participant_id not in self.participants:
                continue
            print(f"    → 发送COMMIT到 {participant_id}...", end=" ")
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
        print(f"    ✓ 事务已提交 ({success_count}/{len(tx_info['participants'])})")
    
    def _complete_abort(self, transaction_id: str, tx_info: dict):
        """完成中止操作"""
        abort_msg = Message(MessageType.ABORT, transaction_id, tx_info['data'])
        success_count = 0
        
        for participant_id in tx_info['participants']:
            if participant_id not in self.participants:
                continue
            print(f"    → 发送ABORT到 {participant_id}...", end=" ")
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
        print(f"    ✓ 事务已中止 ({success_count}/{len(tx_info['participants'])})")
    
    def _command_interface(self):
        """命令行界面"""
        print("\n可用命令:")
        print("  list    - 列出所有参与者")
        print("  tx      - 发起新事务")
        print("  crash   - 模拟崩溃")
        print("  recover - 从崩溃中恢复")
        print("  status  - 查看事务状态")
        print("  quit    - 退出")
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
                    print("未知命令，请使用: list, tx, crash, recover, status, quit")
            except KeyboardInterrupt:
                print("\n使用 'quit' 命令退出")
            except Exception as e:
                print(f"错误: {e}")
    
    def _handle_crash(self):
        """处理崩溃命令"""
        if self.crashed:
            print("已经处于崩溃状态")
            return
        
        self.crashed = True
        print(f"\n💥 协调者已崩溃！")
        print("  - 无法发起新事务")
        print("  - 未完成的事务将被挂起")
        print("  - 参与者可能处于等待状态")
        print("  - 使用 'recover' 命令恢复")
    
    def _handle_recover(self):
        """处理恢复命令"""
        if not self.crashed:
            print("当前未处于崩溃状态")
            return
        
        self._recover_coordinator()
    
    def _list_participants(self):
        """列出所有参与者"""
        print(f"\n已注册参与者 ({len(self.participants)}):")
        if self.participants:
            for pid, (host, port) in self.participants.items():
                print(f"  - {pid} ({host}:{port})")
        else:
            print("  (无)")
    
    def _start_transaction(self):
        """发起新事务"""
        print("\n请输入事务数据 (格式: key=value, 例: account=alice,amount=100):")
        data_str = input("data> ").strip()
        
        if not data_str:
            print("事务数据不能为空")
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
            print("✓ 事务已在后台启动，你可以随时输入 'crash' 命令模拟崩溃")
        else:
            print("无效的数据格式")
    
    def _show_status(self):
        """显示事务状态"""
        print(f"\n事务历史 ({len(self.transactions)}):")
        if self.transactions:
            for tx_id, tx_info in self.transactions.items():
                print(f"  {tx_id}: {tx_info['status']} - {tx_info['data']}")
        else:
            print("  (无)")
    
    def stop(self):
        """停止协调者"""
        print("\n正在关闭协调者...")
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

