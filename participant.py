"""
2PC协议 - 参与者（Participant）
"""
import socket
import threading
import time
import random
from protocol import Message, MessageType


class Participant:
    """参与者类"""
    
    def __init__(self, participant_id: str, host: str = 'localhost', port: int = 6000,
                 coordinator_host: str = 'localhost', coordinator_port: int = 5000,
                 failure_rate: float = 0.0):
        self.participant_id = participant_id
        self.host = host
        self.port = port
        self.coordinator_host = coordinator_host
        self.coordinator_port = coordinator_port
        self.failure_rate = failure_rate  # 模拟失败率 (0.0 - 1.0)
        
        self.prepared_transactions = set()  # 已准备的事务
        self.committed_transactions = {}    # 已提交的事务
        self.aborted_transactions = set()   # 已中止的事务
        
        self.running = False
        self.crashed = False  # crash状态标志
        self.server_socket = None
        self.lock = threading.Lock()
        self.pending_vote = None  # 存储待投票的事务信息 (transaction_id, data)
        self.pending_commit = None  # 存储待确认的COMMIT (transaction_id, data)
        self.pending_abort = None  # 存储待确认的ABORT (transaction_id, data)
        
    def start(self):
        """启动参与者"""
        # 启动服务器
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.running = True
        
        print(f"✓ 参与者 '{self.participant_id}' 启动在 {self.host}:{self.port}")
        
        # 注册到协调者
        if self._register_to_coordinator():
            print(f"✓ 已注册到协调者 {self.coordinator_host}:{self.coordinator_port}")
        else:
            print(f"✗ 注册到协调者失败")
        
        print("=" * 60)
        
        # 启动监听线程
        listen_thread = threading.Thread(target=self._listen_for_requests)
        listen_thread.daemon = True # 设置为守护线程
        listen_thread.start()
        
        # 命令行界面
        self._command_interface()
    
    def _register_to_coordinator(self) -> bool:
        """向协调者注册"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((self.coordinator_host, self.coordinator_port))
            
            # 发送注册信息
            register_msg = f"REGISTER|{self.participant_id}|{self.host}|{self.port}"
            sock.sendall(register_msg.encode('utf-8'))
            
            response = sock.recv(1024).decode('utf-8')
            sock.close()
            
            return response == "OK"
        except Exception as e:
            print(f"注册失败: {e}")
            return False
    
    def _listen_for_requests(self):
        """监听协调者的请求"""
        while self.running:
            try:
                self.server_socket.settimeout(1.0)
                client_socket, addr = self.server_socket.accept()
                threading.Thread(
                    target=self._handle_request,
                    args=(client_socket,),
                    daemon=True # 设置为守护线程
                ).start()
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    print(f"监听错误: {e}")
    
    def _handle_request(self, client_socket):
        """处理协调者的请求"""
        try:
            data = client_socket.recv(4096).decode('utf-8')
            if not data:
                return
            
            # 如果crashed，不处理任何消息
            if self.crashed:
                print(f"  💥 已崩溃，忽略消息")
                return
            
            message = Message.from_json(data)
            response = self._process_message(message)
            
            if response:
                client_socket.sendall(response.to_json().encode('utf-8'))
        except Exception as e:
            print(f"处理请求错误: {e}")
        finally:
            client_socket.close()
    
    def _process_message(self, message: Message) -> Message:
        """处理消息"""
        print(f"\n← 收到: {message.msg_type.value} (事务 {message.transaction_id})")
        
        # 模拟失败
        if self.failure_rate > 0 and random.random() < self.failure_rate:
            print(f"  💥 模拟失败 (失败率: {self.failure_rate*100}%)")
            if message.msg_type == MessageType.PREPARE:
                return Message(MessageType.VOTE_NO, message.transaction_id)
            return None
        
        if message.msg_type == MessageType.PREPARE:
            return self._handle_prepare(message)
        elif message.msg_type == MessageType.COMMIT:
            return self._handle_commit(message)
        elif message.msg_type == MessageType.ABORT:
            return self._handle_abort(message)
        elif message.msg_type == MessageType.QUERY_STATE:
            return self._handle_query_state(message)
        
        return None
    
    def _handle_prepare(self, message: Message) -> Message:
        """处理准备请求 - 等待手动投票"""
        transaction_data = message.data
        
        # 如果有模拟失败率，检查是否自动拒绝
        if self.failure_rate > 0 and random.random() < self.failure_rate:
            print(f"  💥 模拟失败 (失败率: {self.failure_rate*100}%)")
            print(f"  自动投票 NO")
            return Message(MessageType.VOTE_NO, message.transaction_id)
        
        # 保存待投票事务，等待用户手动投票
        with self.lock:
            self.pending_vote = (message.transaction_id, transaction_data)
        
        print(f"  📋 事务数据: {transaction_data}")
        print(f"  ⏳ 等待投票决策...")
        print(f"  请输入命令: vote yes 或 vote no")
        
        # 启动一个线程等待投票，30秒后超时自动投NO
        threading.Thread(
            target=self._wait_for_vote,
            args=(message.transaction_id,),
            daemon=True
        ).start()
        
        # 返回None表示暂不响应，等待用户投票
        return None
    
    def _wait_for_vote(self, transaction_id: str, timeout: int = 30):
        """等待投票，超时自动投NO"""
        time.sleep(timeout)
        with self.lock:
            if self.pending_vote and self.pending_vote[0] == transaction_id:
                print(f"\n⏰ 投票超时！自动投票 NO")
                self._send_vote_to_coordinator(transaction_id, False)
                self.pending_vote = None
    
    def _wait_for_ack_commit(self, transaction_id: str, timeout: int = 30):
        """等待COMMIT确认，超时自动ACK"""
        time.sleep(timeout)
        with self.lock:
            if self.pending_commit and self.pending_commit[0] == transaction_id:
                print(f"\n⏰ 确认超时！自动ACK COMMIT")
                self._send_ack_to_coordinator(transaction_id, MessageType.ACK_COMMIT)
                # 执行提交
                if transaction_id in self.prepared_transactions:
                    self.committed_transactions[transaction_id] = self.pending_commit[1]
                    self.prepared_transactions.remove(transaction_id)
                self.pending_commit = None
    
    def _wait_for_ack_abort(self, transaction_id: str, timeout: int = 30):
        """等待ABORT确认，超时自动ACK"""
        time.sleep(timeout)
        with self.lock:
            if self.pending_abort and self.pending_abort[0] == transaction_id:
                print(f"\n⏰ 确认超时！自动ACK ABORT")
                self._send_ack_to_coordinator(transaction_id, MessageType.ACK_ABORT)
                # 执行中止
                if transaction_id in self.prepared_transactions:
                    self.prepared_transactions.remove(transaction_id)
                self.aborted_transactions.add(transaction_id)
                self.pending_abort = None
    
    def _handle_commit(self, message: Message) -> Message:
        """处理提交请求 - 需要手动确认"""
        transaction_id = message.transaction_id
        transaction_data = message.data
        
        with self.lock:
            if transaction_id not in self.prepared_transactions:
                print(f"  ✗ 事务未准备，拒绝提交")
                return Message(MessageType.ACK_ABORT, transaction_id)
            
            # 保存待确认的COMMIT
            self.pending_commit = (transaction_id, transaction_data)
        
        print(f"  📋 收到COMMIT请求")
        print(f"  事务数据: {transaction_data}")
        print(f"  ⏳ 等待确认...")
        print(f"  请输入命令: ack commit 或 ack abort")
        
        # 启动超时线程（30秒后自动ACK）
        threading.Thread(
            target=self._wait_for_ack_commit,
            args=(transaction_id,),
            daemon=True
        ).start()
        
        return None  # 不立即响应，等待手动确认
    
    def _handle_abort(self, message: Message) -> Message:
        """处理中止请求 - 需要手动确认"""
        transaction_id = message.transaction_id
        transaction_data = message.data
        
        with self.lock:
            # 保存待确认的ABORT
            self.pending_abort = (transaction_id, transaction_data)
        
        print(f"  📋 收到ABORT请求")
        print(f"  事务数据: {transaction_data}")
        print(f"  ⏳ 等待确认...")
        print(f"  请输入命令: ack abort")
        
        # 启动超时线程（30秒后自动ACK）
        threading.Thread(
            target=self._wait_for_ack_abort,
            args=(transaction_id,),
            daemon=True
        ).start()
        
        return None  # 不立即响应，等待手动确认
    
    def _handle_query_state(self, message: Message) -> Message:
        """处理状态查询"""
        transaction_id = message.transaction_id
        
        with self.lock:
            # 检查事务状态
            if transaction_id in self.committed_transactions:
                status = 'COMMITTED'
                data = self.committed_transactions[transaction_id]
            elif transaction_id in self.prepared_transactions:
                status = 'PREPARED'
                data = {}
            elif transaction_id in self.aborted_transactions:
                status = 'ABORTED'
                data = {}
            else:
                status = 'UNKNOWN'
                data = {}
        
        print(f"  状态查询: {status}")
        return Message(MessageType.STATE_RESPONSE, transaction_id, {'status': status, 'data': data})
    
    def _validate_transaction(self, data: dict) -> bool:
        """验证事务（可以自定义验证逻辑）"""
        # 示例：简单检查是否有数据
        return len(data) > 0
    
    def _send_vote_to_coordinator(self, transaction_id: str, vote_yes: bool):
        """向协调者发送投票"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((self.coordinator_host, self.coordinator_port))
            
            # 发送投票消息
            if vote_yes:
                with self.lock:
                    self.prepared_transactions.add(transaction_id)
                vote_msg = Message(MessageType.VOTE_YES, transaction_id)
                print(f"  ✓ 已投票 YES")
            else:
                vote_msg = Message(MessageType.VOTE_NO, transaction_id)
                print(f"  ✗ 已投票 NO")
            
            # 使用特殊标记表示这是一个延迟的投票响应
            vote_data = f"VOTE_RESPONSE|{self.participant_id}|{vote_msg.to_json()}"
            sock.sendall(vote_data.encode('utf-8'))
            sock.close()
        except Exception as e:
            print(f"发送投票失败: {e}")
    
    def _send_ack_to_coordinator(self, transaction_id: str, ack_type: MessageType):
        """向协调者发送ACK确认"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((self.coordinator_host, self.coordinator_port))
            
            ack_msg = Message(ack_type, transaction_id)
            
            # 使用特殊标记表示这是一个延迟的ACK响应
            ack_data = f"ACK_RESPONSE|{self.participant_id}|{ack_msg.to_json()}"
            sock.sendall(ack_data.encode('utf-8'))
            sock.close()
            
            if ack_type == MessageType.ACK_COMMIT:
                print(f"  ✓ 已确认 COMMIT")
            else:
                print(f"  ✓ 已确认 ABORT")
        except Exception as e:
            print(f"发送ACK失败: {e}")
    
    def _request_history_from_coordinator(self):
        """从协调者请求历史日志"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((self.coordinator_host, self.coordinator_port))
            
            # 发送历史请求
            history_msg = Message(MessageType.REQUEST_HISTORY, "HISTORY", {"participant_id": self.participant_id})
            request_data = f"HISTORY_REQUEST|{self.participant_id}|{history_msg.to_json()}"
            sock.sendall(request_data.encode('utf-8'))
            
            # 接收历史数据
            response_data = sock.recv(65536).decode('utf-8')
            sock.close()
            
            if response_data:
                response = Message.from_json(response_data)
                if response.msg_type == MessageType.HISTORY_RESPONSE:
                    history = response.data.get('history', [])
                    print(f"\n📜 从协调者获取到 {len(history)} 条历史记录")
                    
                    # 同步历史数据
                    with self.lock:
                        for record in history:
                            tx_id = record['transaction_id']
                            status = record['status']
                            data = record['data']
                            
                            if status == 'COMMITTED':
                                self.committed_transactions[tx_id] = data
                                if tx_id in self.prepared_transactions:
                                    self.prepared_transactions.remove(tx_id)
                            elif status == 'ABORTED':
                                self.aborted_transactions.add(tx_id)
                                if tx_id in self.prepared_transactions:
                                    self.prepared_transactions.remove(tx_id)
                    
                    print(f"  ✓ 历史数据已同步")
                    return True
        except Exception as e:
            print(f"请求历史失败: {e}")
        return False
    
    def _command_interface(self):
        """命令行界面"""
        print("\n可用命令:")
        print("  status            - 查看状态")
        print("  data              - 查看已提交数据")
        print("  vote yes/no       - 对待投票事务投票")
        print("  ack commit/abort  - 确认COMMIT或ABORT")
        print("  crash             - 模拟崩溃")
        print("  recover           - 从崩溃中恢复")
        print("  fail              - 设置失败率")
        print("  quit              - 退出")
        print()
        
        while self.running:
            try:
                status_prefix = "💥CRASHED" if self.crashed else self.participant_id
                cmd = input(f"{status_prefix}> ").strip()
                
                if not cmd:
                    continue
                
                cmd_lower = cmd.lower()
                
                if cmd_lower == 'quit':
                    self.stop()
                    break
                elif cmd_lower == 'status':
                    self._show_status()
                elif cmd_lower == 'data':
                    self._show_data()
                elif cmd_lower.startswith('vote '):
                    self._handle_vote_command(cmd)
                elif cmd_lower.startswith('ack '):
                    self._handle_ack_command(cmd)
                elif cmd_lower == 'crash':
                    self._handle_crash()
                elif cmd_lower == 'recover':
                    self._handle_recover()
                elif cmd_lower == 'fail':
                    self._set_failure_rate()
                else:
                    print("未知命令，请使用: status, data, vote yes/no, ack commit/abort, crash, recover, fail, quit")
            except KeyboardInterrupt:
                print("\n使用 'quit' 命令退出")
            except Exception as e:
                print(f"错误: {e}")
    
    def _handle_vote_command(self, cmd: str):
        """处理投票命令"""
        parts = cmd.strip().lower().split()
        if len(parts) != 2 or parts[1] not in ['yes', 'no']:
            print("用法: vote yes 或 vote no")
            return
        
        with self.lock:
            if not self.pending_vote:
                print("没有待投票的事务")
                return
            
            transaction_id, data = self.pending_vote
            vote_yes = (parts[1] == 'yes')
            self.pending_vote = None
        
        print(f"\n投票事务 {transaction_id}")
        print(f"  数据: {data}")
        self._send_vote_to_coordinator(transaction_id, vote_yes)
    
    def _handle_ack_command(self, cmd: str):
        """处理ACK确认命令"""
        parts = cmd.strip().lower().split()
        if len(parts) != 2 or parts[1] not in ['commit', 'abort']:
            print("用法: ack commit 或 ack abort")
            return
        
        ack_commit = (parts[1] == 'commit')
        
        with self.lock:
            # 检查是否有待确认的COMMIT或ABORT
            if ack_commit:
                if not self.pending_commit:
                    print("没有待确认的COMMIT")
                    return
                transaction_id, data = self.pending_commit
                self.pending_commit = None
                
                # 执行提交
                if transaction_id in self.prepared_transactions:
                    self.committed_transactions[transaction_id] = data
                    self.prepared_transactions.remove(transaction_id)
            else:
                # 用户可以对COMMIT请求回复abort，或对ABORT请求确认
                if self.pending_commit:
                    transaction_id, data = self.pending_commit
                    self.pending_commit = None
                elif self.pending_abort:
                    transaction_id, data = self.pending_abort
                    self.pending_abort = None
                else:
                    print("没有待确认的COMMIT或ABORT")
                    return
                
                # 执行中止
                if transaction_id in self.prepared_transactions:
                    self.prepared_transactions.remove(transaction_id)
                self.aborted_transactions.add(transaction_id)
        
        print(f"\n确认事务 {transaction_id}")
        print(f"  数据: {data}")
        
        # 发送ACK
        if ack_commit:
            self._send_ack_to_coordinator(transaction_id, MessageType.ACK_COMMIT)
        else:
            self._send_ack_to_coordinator(transaction_id, MessageType.ACK_ABORT)
    
    def _handle_crash(self):
        """处理崩溃命令"""
        if self.crashed:
            print("已经处于崩溃状态")
            return
        
        self.crashed = True
        print(f"\n💥 {self.participant_id} 已崩溃！")
        print("  - 将不再接收和处理任何消息")
        print("  - 使用 'recover' 命令恢复")
    
    def _handle_recover(self):
        """处理恢复命令"""
        if not self.crashed:
            print("当前未处于崩溃状态")
            return
        
        print(f"\n🔄 开始恢复 {self.participant_id}...")
        
        # 重新注册到协调者
        if self._register_to_coordinator():
            print(f"  ✓ 已重新注册到协调者")
        else:
            print(f"  ✗ 重新注册失败")
            return
        
        # 请求历史日志
        print("  📡 正在请求历史日志...")
        if self._request_history_from_coordinator():
            self.crashed = False
            print(f"\n✓ {self.participant_id} 已完全恢复！")
        else:
            print(f"  ✗ 历史同步失败，但已标记为恢复状态")
            self.crashed = False
    
    def _show_status(self):
        """显示状态"""
        print(f"\n参与者状态:")
        print(f"  ID: {self.participant_id}")
        print(f"  地址: {self.host}:{self.port}")
        print(f"  状态: {'💥 已崩溃' if self.crashed else '✓ 正常运行'}")
        print(f"  失败率: {self.failure_rate*100}%")
        
        with self.lock:
            has_pending = self.pending_vote is not None
            if has_pending:
                tx_id, data = self.pending_vote
                print(f"  待投票事务: {tx_id} - {data}")
        
        print(f"  已准备事务: {len(self.prepared_transactions)}")
        print(f"  已提交事务: {len(self.committed_transactions)}")
        print(f"  已中止事务: {len(self.aborted_transactions)}")
    
    def _show_data(self):
        """显示已提交的数据"""
        print(f"\n已提交的事务数据 ({len(self.committed_transactions)}):")
        if self.committed_transactions:
            for tx_id, data in self.committed_transactions.items():
                print(f"  {tx_id}: {data}")
        else:
            print("  (无)")
    
    def _set_failure_rate(self):
        """设置失败率"""
        try:
            rate = float(input("输入失败率 (0.0-1.0): "))
            if 0.0 <= rate <= 1.0:
                self.failure_rate = rate
                print(f"✓ 失败率已设置为 {rate*100}%")
            else:
                print("失败率必须在0.0-1.0之间")
        except ValueError:
            print("无效的数值")
    
    def stop(self):
        """停止参与者"""
        print(f"\n正在关闭参与者 {self.participant_id}...")
        self.running = False
        if self.server_socket:
            self.server_socket.close()


def main():
    import sys
    
    if len(sys.argv) < 2:
        print("用法: python participant.py <participant_id> [port] [coordinator_port]")
        print("示例: python participant.py P1 6001 5000")
        sys.exit(1)
    
    participant_id = sys.argv[1]
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 6000
    coordinator_port = int(sys.argv[3]) if len(sys.argv) > 3 else 5000
    
    participant = Participant(
        participant_id=participant_id,
        port=port,
        coordinator_port=coordinator_port
    )
    
    try:
        participant.start()
    except KeyboardInterrupt:
        participant.stop()


if __name__ == '__main__':
    main()

