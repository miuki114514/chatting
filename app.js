// ============================================
// 自由论坛 - P2P 实时共享社区 (MQTT 版)
// 零配置，开箱即用，无需服务器
// ============================================

// 管理员邀请码（可自行修改）
const OWNER_CODE = 'MIUQI-OWNER-2026';
const ADMIN_CODE = 'MIUQI-ADMIN-2026';

class ForumApp {
    constructor() {
        this.currentUser = null;
        this.currentPage = 'home';
        this.currentFriendId = null;
        this.currentPostId = null;
        this.currentSearchQuery = '';
        this.posts = [];
        this.users = {};
        this.comments = [];
        this.messages = {};
        this.friendRequests = [];
        this.groups = [];
        this.groupMessages = {};
        this.deletedPostIds = [];
        this.deletedCommentIds = [];
        try {
            this.favorites = JSON.parse(localStorage.getItem('favorites') || '[]');
        } catch (e) {
            console.warn('收藏数据损坏，已重置');
            this.favorites = [];
            localStorage.setItem('favorites', '[]');
        }
        this.client = null;
        this.connected = false;
        this.myClientId = 'forum_' + Math.random().toString(36).substr(2, 9);
        this.loadLocalData();
        this.init();
    }

    init() {
        try {
            this.checkLoginStatus();
        } catch (e) {
            console.error('登录状态检查失败:', e);
        }

        const hasPendingUser = Object.values(this.users).some(u => u.role === 'pending');

        if (this.currentUser) {
            const fullUser = this.users[this.currentUser.id];
            if (fullUser && fullUser.role === 'pending') {
                this.currentUser = null;
                localStorage.removeItem('currentUser');
                console.log('当前用户为 pending 状态，清除登录状态');
                this.router('login');
            } else {
                try {
                    this.connectMQTT();
                } catch (e) {
                    console.error('MQTT 连接失败:', e);
                }
                try {
                    this.router('home');
                } catch (e) {
                    console.error('页面渲染失败:', e);
                }
            }
        } else {
            if (hasPendingUser) {
                console.log('检测到待审核用户，连接 MQTT 以同步注册信息');
                try {
                    this.connectMQTT(() => {
                        const pendingUsers = Object.values(this.users).filter(u => u.role === 'pending');
                        pendingUsers.forEach((user, idx) => {
                            setTimeout(() => {
                                this.publish(`forum/users/${user.id}`, { type: 'user', data: user }, true);
                            }, idx * 300);
                        });
                    });
                } catch (e) {
                    console.error('pending 用户 MQTT 连接失败:', e);
                }
            }
            try {
                this.router('login');
            } catch (e) {
                console.error('页面渲染失败:', e);
            }
        }
    }

    // ========== 本地数据持久化 ==========
    loadLocalData() {
        try {
            const data = localStorage.getItem('forum_data');
            if (data) {
                const parsed = JSON.parse(data);
                this.posts = parsed.posts || [];
                this.users = parsed.users || {};
                this.comments = parsed.comments || [];
                this.messages = parsed.messages || {};
                this.friendRequests = parsed.friendRequests || [];
                this.groups = parsed.groups || [];
                this.groupMessages = parsed.groupMessages || {};
                this.deletedPostIds = parsed.deletedPostIds || [];
                this.deletedCommentIds = parsed.deletedCommentIds || [];
                // 强力过滤：确保posts中不包含已删除帖子
                if (this.deletedPostIds.length > 0) {
                    const beforeLen = this.posts.length;
                    this.posts = this.posts.filter(p => !this.deletedPostIds.includes(p.id));
                    if (beforeLen !== this.posts.length) {
                        console.log('🧹 从本地数据中清除了', beforeLen - this.posts.length, '个已删除帖子');
                    }
                }
                // 强力过滤：确保comments中不包含已删除评论
                if (this.deletedCommentIds.length > 0) {
                    const beforeLen = this.comments.length;
                    this.comments = this.comments.filter(c => !this.deletedCommentIds.includes(c.id));
                    if (beforeLen !== this.comments.length) {
                        console.log('🧹 从本地数据中清除了', beforeLen - this.comments.length, '个已删除评论');
                    }
                }
                Object.values(this.users).forEach(u => {
                    if (!u.role) u.role = 'user';
                    if (typeof u.banned === 'undefined') u.banned = false;
                });
                const newMessages = {};
                let migrated = 0;
                Object.keys(this.messages).forEach(key => {
                    if (key.includes('|')) {
                        newMessages[key] = this.messages[key];
                    } else {
                        const parts = key.split('_');
                        if (parts.length >= 4) {
                            const allIds = [];
                            for (let i = 0; i < parts.length; i++) {
                                if (parts[i] === 'user' && i + 1 < parts.length) {
                                    allIds.push('user_' + parts[i + 1]);
                                    i++;
                                }
                            }
                            if (allIds.length === 2) {
                                const newKey = [allIds[0], allIds[1]].sort().join('|');
                                newMessages[newKey] = this.messages[key];
                                migrated++;
                            }
                        }
                    }
                });
                if (migrated > 0) {
                    this.messages = newMessages;
                    try { this.saveLocalData(); } catch (e) {}
                    console.log('📦 迁移了', migrated, '个聊天记录的 key');
                }
            }
        } catch (e) {
            console.error('本地数据加载失败，已重置:', e);
            this.posts = [];
            this.users = {};
            this.comments = [];
            this.messages = {};
            this.friendRequests = [];
            this.groups = [];
            this.groupMessages = {};
            this.deletedPostIds = [];
            this.deletedCommentIds = [];
        }
    }

    saveLocalData() {
        try {
            const data = {
                posts: this.posts,
                users: this.users,
                comments: this.comments,
                messages: this.messages,
                friendRequests: this.friendRequests,
                groups: this.groups,
                groupMessages: this.groupMessages,
                deletedPostIds: this.deletedPostIds,
                deletedCommentIds: this.deletedCommentIds
            };
            localStorage.setItem('forum_data', JSON.stringify(data));
        } catch (e) {
            console.error('保存数据失败:', e);
        }
    }

    // ========== MQTT 连接 ==========
    connectMQTT(onConnectCallback = null) {
        try {
            if (this.client) {
                try { this.client.end(true); } catch (e) {}
                this.client = null;
            }

            const CN_BROKER = 'wss://broker-cn.emqx.io:8084/mqtt';
            const INT_BROKER = 'wss://broker.emqx.io:8084/mqtt';
            const INT_WS = 'ws://broker.emqx.io:8083/mqtt';

            this.myClientId = 'forum_' + Math.random().toString(36).substr(2, 10) + '_' + Date.now();

            let triedInt = false;
            let triedIntWs = false;
            let triedCn = false;
            let alreadySwitched = false;
            let callbackFired = false;

            const fireCallback = () => {
                if (!callbackFired && onConnectCallback) {
                    callbackFired = true;
                    try { onConnectCallback(); } catch (e) { console.error('连接回调异常:', e); }
                }
            };

            const doConnect = (brokerUrl) => {
                console.log('连接到 broker:', brokerUrl);
                this.updateConnectionStatus('🟡 连接中...');

                const options = {
                    clientId: this.myClientId,
                    clean: true,
                    reconnectPeriod: 4000,
                    connectTimeout: 10000,
                    keepalive: 30
                };

                const client = mqtt.connect(brokerUrl, options);
                this.client = client;

                client.on('connect', () => {
                    this.connected = true;
                    this.currentBroker = brokerUrl;
                    const isCn = brokerUrl.includes('broker-cn');
                    const nodeLabel = isCn ? '国内' : '国际';
                    this.updateConnectionStatus('🟢 ' + nodeLabel);
                    console.log('已连接:', brokerUrl);
                    this.subscribeTopics();
                    fireCallback();
                    setTimeout(() => {
                        this.publishAllDataAsRetained();
                        this.flushOfflineQueue();
                        this.requestSync();
                    }, 1000);
                    setTimeout(() => this.requestSync(), 5000);
                    setTimeout(() => this.requestSync(), 12000);
                });

                let chatMsgCount = 0;
                client.on('message', (topic, message) => {
                    try {
                        const payload = JSON.parse(message.toString());
                        if (!payload || !payload.type) {
                            console.warn('收到无效消息:', topic, message.toString().substring(0, 100));
                            return;
                        }
                        chatMsgCount++;
                        const logTypes = ['friendRequest', 'chat_message', 'user', 'post', 'audit_request', 'audit_query', 'audit_ping', 'audit_decision'];
                        if (chatMsgCount % 10 === 0 || logTypes.includes(payload.type)) {
                            const dataStr = payload.data ? JSON.stringify(payload.data).substring(0, 150) : '无数据';
                            console.log('📥 收到消息 [' + chatMsgCount + ']: topic=' + topic + ', type=' + payload.type + ' | ' + dataStr);
                        }
                        this.handleMessage(payload);
                    } catch (e) {
                        console.error('消息解析失败:', topic, e.message);
                    }
                });

                client.on('error', (err) => {
                    console.error('MQTT 错误:', err.message || err);
                    this.connected = false;
                    if (!alreadySwitched && brokerUrl === CN_BROKER && !triedInt) {
                        alreadySwitched = true;
                        triedInt = true;
                        try { client.end(true); } catch (e) {}
                        this.client = null;
                        console.log('国内节点失败，尝试国际节点（可能需要VPN）');
                        setTimeout(() => doConnect(INT_BROKER), 500);
                    } else if (!alreadySwitched && brokerUrl === INT_BROKER && !triedIntWs) {
                        alreadySwitched = true;
                        triedIntWs = true;
                        try { client.end(true); } catch (e) {}
                        this.client = null;
                        console.log('国际 WSS 失败，尝试国际 WS');
                        setTimeout(() => doConnect(INT_WS), 500);
                    } else {
                        this.updateConnectionStatus('🟠 连接错误');
                    }
                });

                client.on('offline', () => {
                    this.connected = false;
                    this.updateConnectionStatus('🔴 离线');
                });

                client.on('reconnect', () => {
                    this.updateConnectionStatus('🟡 重连中...');
                });

                client.on('close', () => {
                    this.connected = false;
                });
            };

            doConnect(CN_BROKER);
        } catch (e) {
            console.error('MQTT 初始化失败:', e);
            this.updateConnectionStatus('❌ 不可用');
        }
    }

    updateConnectionStatus(text) {
        const el = document.getElementById('conn-status');
        if (el) el.textContent = text;

        const tip = document.getElementById('conn-tip');
        if (tip) {
            const isConnected = text.includes('🟢') || text.includes('在线');
            tip.style.display = isConnected ? 'none' : 'block';
        }
    }

    subscribeTopics() {
        const topics = [
            'forum/posts/#',
            'forum/comments/#',
            'forum/users/#',
            'forum/audit/#',
            'forum/ban/#',
            'forum/role/#',
            'forum/friends/#',
            'forum/sync/request',
            `forum/sync/response/${this.myClientId}`
        ];
        if (this.currentUser) {
            topics.push(`forum/msg/${this.currentUser.id}/#`);
            topics.push(`forum/friends/${this.currentUser.id}`);
            topics.push('forum/group/#');
        }
        console.log('📡 订阅 topics:', topics);
        topics.forEach(t => this.client.subscribe(t, { qos: 1 }, (err) => {
            if (err) console.error('订阅失败:', t, err);
        }));
    }

    publish(topic, data, retained = true, callback = null) {
        if (this.connected && this.client) {
            try {
                this.client.publish(topic, JSON.stringify(data), { retain: retained, qos: 1 }, (err) => {
                    if (err) {
                        console.error('MQTT 发布失败:', topic, err);
                        this.queueOfflineMessage(topic, data, retained);
                        if (callback) callback(err);
                    } else {
                        if (callback) callback(null);
                    }
                });
            } catch (err) {
                console.error('发布异常:', err);
                this.queueOfflineMessage(topic, data, retained);
                if (callback) callback(err);
            }
        } else {
            console.log('未连接，消息已存入队列:', topic);
            this.queueOfflineMessage(topic, data, retained);
            if (callback) callback(null);
        }
    }

    queueOfflineMessage(topic, data, retained) {
        try {
            let queue = JSON.parse(localStorage.getItem('mqtt_offline_queue') || '[]');
            queue.push({
                topic,
                data,
                retained,
                timestamp: Date.now()
            });
            if (queue.length > 200) queue = queue.slice(-200);
            localStorage.setItem('mqtt_offline_queue', JSON.stringify(queue));
        } catch (e) {
            console.error('离线消息队列保存失败:', e);
        }
    }

    flushOfflineQueue() {
        if (!this.connected || !this.client) return;
        try {
            const queue = JSON.parse(localStorage.getItem('mqtt_offline_queue') || '[]');
            if (queue.length === 0) return;
            console.log('发送离线消息队列，共', queue.length, '条');
            let sent = 0;
            queue.forEach((item, i) => {
                setTimeout(() => {
                    if (this.connected && this.client) {
                        this.client.publish(item.topic, JSON.stringify(item.data),
                            { retain: item.retained, qos: 1 }, (err) => {
                                if (!err) sent++;
                            });
                    }
                }, i * 100);
            });
            setTimeout(() => {
                localStorage.setItem('mqtt_offline_queue', '[]');
                console.log('离线消息队列处理完成');
            }, (queue.length + 1) * 100 + 500);
        } catch (e) {
            console.error('离线消息队列处理失败:', e);
        }
    }

    manualSync() {
        console.log('手动同步触发');
        this.updateConnectionStatus('🟡 同步中...');
        if (this.client) {
            try { this.client.end(true); } catch (e) {}
            this.client = null;
            this.connected = false;
        }
        setTimeout(() => {
            this.connectMQTT();
        }, 100);
        setTimeout(() => {
            if (this.connected) {
                this.requestSync();
                this.flushOfflineQueue();
                console.log('🔄 手动同步：首次请求');
            }
        }, 3000);
        setTimeout(() => {
            if (this.connected) {
                this.requestSync();
                console.log('🔄 手动同步：二次请求');
            }
        }, 7000);
        setTimeout(() => {
            if (this.connected) {
                this.requestSync();
                console.log('🔄 手动同步：三次请求');
            }
        }, 12000);
    }

    publishAllDataAsRetained() {
        if (!this.connected || !this.client) return;
        try {
            let userCount = 0, frCount = 0, postCount = 0, postSkipped = 0;
            Object.values(this.users).forEach(user => {
                if (user && user.id) {
                    this.publish(`forum/users/${user.id}`, { type: 'user', data: user }, true);
                    userCount++;
                }
            });
            this.friendRequests.forEach(fr => {
                if (fr && fr.id) {
                    if (fr.status === 'accepted' || fr.status === 'rejected' || fr.status === 'pending') {
                        const targetId = fr.from === this.currentUser?.id ? fr.to : fr.from;
                        if (targetId) {
                            this.publish(`forum/friends/${targetId}`, { type: 'friendRequest', data: fr }, true);
                            frCount++;
                        }
                    }
                }
            });
            this.posts.forEach(post => {
                if (post && post.id) {
                    try {
                        const payload = JSON.stringify({ type: 'post', data: post });
                        if (payload.length <= 700 * 1024) {
                            this.publish(`forum/posts/${post.id}`, { type: 'post', data: post }, true);
                            postCount++;
                        } else {
                            postSkipped++;
                        }
                    } catch (e) {
                        postSkipped++;
                    }
                }
            });
            console.log(`已同步: ${userCount}用户, ${frCount}好友申请, ${postCount}帖子${postSkipped > 0 ? ' (跳过' + postSkipped + '个过大帖子)' : ''}`);
        } catch (e) {
            console.error('批量发布失败:', e);
        }
    }

    requestSync() {
        this.publish('forum/sync/request', {
            type: 'sync_request',
            clientId: this.myClientId,
            timestamp: Date.now()
        }, false);
    }

    // ========== 消息处理 ==========
    handleMessage(payload) {
        switch (payload.type) {
            case 'post':
            case 'post_state':
                this.mergePost(payload.data);
                break;
            case 'comment':
                this.mergeComment(payload.data);
                break;
            case 'user':
                this.mergeUser(payload.data);
                break;
            case 'audit_decision':
                this.applyAuditDecision(payload);
                break;
            case 'audit_request':
                this.handleAuditRequest(payload);
                break;
            case 'audit_query':
                this.handleAuditQuery(payload);
                break;
            case 'audit_ping':
                this.handleAuditPing(payload);
                break;
            case 'user_ban':
                this.applyUserBan(payload);
                break;
            case 'role_change':
                this.applyRoleChange(payload);
                break;
            case 'sync_request':
                if (payload.clientId !== this.myClientId) {
                    setTimeout(() => {
                        this.publishAllDataAsRetained();
                        this.sendSyncResponse(payload.clientId);
                    }, 500);
                }
                break;
            case 'sync_response':
                if (payload.targetId === this.myClientId) {
                    this.applySyncData(payload.data);
                }
                break;
            case 'chat_message':
                this.handleChatMessage(payload.data);
                break;
            case 'group_message':
                this.handleGroupMessage(payload.data);
                break;
            case 'group_meta':
                this.mergeGroupMeta(payload.data);
                break;
            case 'friendRequest':
                this.mergeFriendRequest(payload.data);
                break;
        }
    }

    mergePost(post) {
        if (!post || !post.id) return;
        if (post._deleted) {
            const existingIdx = this.posts.findIndex(p => p.id === post.id);
            if (existingIdx >= 0) {
                this.posts.splice(existingIdx, 1);
                this.comments = this.comments.filter(c => c.postId !== post.id);
                this.favorites = this.favorites.filter(id => id !== post.id);
            }
            if (!this.deletedPostIds.includes(post.id)) {
                this.deletedPostIds.push(post.id);
            }
            this.saveLocalData();
            console.log('🗑️ 收到帖子删除通知:', post.id);
            if (this.currentPage === 'post' && this.currentPostId === post.id) {
                this.router('home');
            }
            if (this.currentPage === 'home' || this.currentPage === 'explore' || this.currentPage === 'profile') {
                this.renderCurrentPage();
            }
            return;
        }
        if (this.deletedPostIds.includes(post.id)) {
            return;
        }
        const existingIdx = this.posts.findIndex(p => p.id === post.id);
        let changed = false;
        if (existingIdx >= 0) {
            const existing = this.posts[existingIdx];
            const newTimestamp = post.updatedAt || post.timestamp || 0;
            const oldTimestamp = existing.updatedAt || existing.timestamp || 0;
            if (newTimestamp >= oldTimestamp) {
                if (post._state_only) {
                    existing.isPinned = post.isPinned;
                    existing.updatedAt = post.updatedAt;
                    console.log('📌 收到帖子置顶状态变更:', post.id, '新状态:', post.isPinned ? '已置顶' : '未置顶');
                } else {
                    this.posts[existingIdx] = post;
                    if (existing.isPinned !== post.isPinned) {
                        console.log('📌 收到帖子置顶状态变更:', post.id, '新状态:', post.isPinned ? '已置顶' : '未置顶');
                    }
                }
                changed = true;
            }
        } else {
            if (!post._state_only) {
                this.posts.push(post);
                changed = true;
            }
        }
        if (changed) {
            this.saveLocalData();
            if (this.currentPage === 'home' || this.currentPage === 'explore' || this.currentPage === 'profile' || this.currentPage === 'post') {
                this.renderCurrentPage();
            }
        }
    }

    mergeComment(comment) {
        if (!comment || !comment.id) return;
        if (comment._deleted) {
            const existingIdx = this.comments.findIndex(c => c.id === comment.id);
            if (existingIdx >= 0) {
                this.comments.splice(existingIdx, 1);
            }
            if (!this.deletedCommentIds.includes(comment.id)) {
                this.deletedCommentIds.push(comment.id);
            }
            this.saveLocalData();
            console.log('🗑️ 收到评论删除通知:', comment.id);
            return;
        }
        if (this.deletedCommentIds.includes(comment.id)) {
            const newTs = comment.updatedAt || comment.timestamp || 0;
            const existingComment = this.comments.find(c => c.id === comment.id);
            const oldTs = existingComment ? (existingComment.updatedAt || existingComment.timestamp || 0) : 0;
            if (newTs >= oldTs && comment.content) {
                this.deletedCommentIds = this.deletedCommentIds.filter(id => id !== comment.id);
                console.log('📝 评论从删除列表恢复:', comment.id);
            } else {
                return;
            }
        }
        const existingIdx = this.comments.findIndex(c => c.id === comment.id);
        let changed = false;
        if (existingIdx >= 0) {
            const existing = this.comments[existingIdx];
            const newTs = comment.updatedAt || comment.timestamp || 0;
            const oldTs = existing.updatedAt || existing.timestamp || 0;
            if (newTs >= oldTs) {
                this.comments[existingIdx] = comment;
                changed = true;
            }
        } else {
            this.comments.push(comment);
            changed = true;
        }
        if (changed) {
            this.saveLocalData();
            if (this.currentPage === 'post' && this.currentPostId === comment.postId) {
                this.renderCurrentPage();
            }
        }
    }

    mergeUser(user) {
        if (!user || !user.id) return;
        if (user.role === 'owner') {
            const existingOwner = Object.values(this.users).find(u => u.role === 'owner' && u.id !== user.id);
            if (existingOwner) {
                user.role = 'admin';
            }
        }
        const existing = this.users[user.id];
        let shouldMerge = false;
        let reason = '';
        if (!existing) {
            shouldMerge = true;
            reason = '新用户数据';
        } else {
            const oldTs = existing.updatedAt || 0;
            const newTs = user.updatedAt || 0;
            if (newTs >= oldTs) {
                shouldMerge = true;
                reason = '时间戳更新 (' + newTs + ' >= ' + oldTs + ')';
            }
            if (!shouldMerge && (existing.avatar !== user.avatar || existing.nickname !== user.nickname)) {
                shouldMerge = true;
                reason = '头像/昵称变更';
            }
            if (!shouldMerge && existing.banned !== user.banned) {
                shouldMerge = true;
                reason = '封禁状态变更: ' + (existing.banned ? '已封禁→已解封' : '未封禁→已封禁');
            }
            if (!shouldMerge && existing.hideProfile !== user.hideProfile) {
                shouldMerge = true;
                reason = '资料隐藏状态变更: ' + (existing.hideProfile ? '已隐藏→公开' : '公开→已隐藏');
            }
            if (!shouldMerge && existing.role !== user.role && user.role !== 'pending') {
                shouldMerge = true;
                reason = '角色变更: ' + existing.role + ' → ' + user.role;
            }
            if (user.role === 'pending' && existing.role !== 'pending' && !shouldMerge) {
                shouldMerge = true;
                reason = 'pending 状态强制更新';
            }
        }
        if (shouldMerge) {
            this.users[user.id] = user;
            this.saveLocalData();
            this.updateUserUI();
            console.log('👤 用户数据已同步:', user.nickname || user.id, '原因:', reason);
        }
    }

    // ========== 管理员审核机制 ==========
    getOwner() {
        return Object.values(this.users).find(u => u.role === 'owner');
    }

    isAdmin(userId) {
        const user = this.users[userId];
        return user && (user.role === 'admin' || user.role === 'owner');
    }

    isOwner(userId) {
        const user = this.users[userId];
        return user && user.role === 'owner';
    }

    getValidPosts() {
        return this.posts.filter(p => p && !this.deletedPostIds.includes(p.id));
    }

    getValidComments(postId) {
        return this.comments.filter(c => c && c.postId === postId && !this.deletedCommentIds.includes(c.id));
    }

    handleAuditRequest(payload) {
        if (!this.currentUser || !this.isAdmin(this.currentUser.id)) return;
        const { userId, nickname, username } = payload;
        if (!userId) return;
        const existingUser = this.users[userId];
        if (existingUser && existingUser.role !== 'pending') return;
        console.log('收到新的审核申请:', nickname, username);
        if (this.currentPage === 'admin') {
            this.renderCurrentPage();
        } else {
            this.showPendingAuditNotice = true;
        }
    }

    handleAuditQuery(payload) {
        const pendingUsers = Object.values(this.users).filter(u => u.role === 'pending');
        if (pendingUsers.length === 0) return;
        console.log('收到管理员查询审核申请，有', pendingUsers.length, '个待审核用户');
        pendingUsers.forEach((user, idx) => {
            setTimeout(() => {
                user.updatedAt = Date.now();
                this.users[user.id] = user;
                this.saveLocalData();
                this.publish(`forum/users/${user.id}`, { type: 'user', data: user }, true);
                this.publish('forum/audit/ping', {
                    type: 'audit_ping',
                    userId: user.id,
                    nickname: user.nickname,
                    username: user.username,
                    timestamp: Date.now()
                }, false);
            }, idx * 400);
        });
    }

    handleAuditPing(payload) {
        if (!this.currentUser || !this.isAdmin(this.currentUser.id)) return;
        const { userId, nickname, username } = payload;
        if (!userId) return;
        console.log('收到待审核用户的回应:', nickname, username);
        if (this.currentPage === 'admin') {
            setTimeout(() => this.renderCurrentPage(), 200);
        }
    }

    applyAuditDecision(payload) {
        const { userId, decision, by } = payload;
        const user = this.users[userId];
        if (!user) return;
        const oldRole = user.role;
        if (decision === 'approved') {
            user.role = 'user';
            user.approvedBy = by;
            user.approvedAt = Date.now();
        } else if (decision === 'rejected') {
            user.role = 'rejected';
            user.approvedBy = by;
            user.approvedAt = Date.now();
        }
        user.updatedAt = Date.now();
        this.saveLocalData();
        const isCurrentUser = this.currentUser && this.currentUser.id === userId;
        if (isCurrentUser) {
            this.currentUser = { id: user.id, username: user.username, nickname: user.nickname };
            localStorage.setItem('currentUser', JSON.stringify(this.currentUser));
            this.updateUserUI();
            if (decision === 'approved' && oldRole === 'pending') {
                setTimeout(() => {
                    alert('🎉 恭喜！你的账号已通过审核！\n\n现在可以正常发帖、评论和浏览完整内容了。');
                    this.router('home');
                }, 300);
            } else if (decision === 'rejected' && oldRole === 'pending') {
                setTimeout(() => {
                    alert('❌ 你的注册申请未通过审核。');
                    this.currentUser = null;
                    localStorage.removeItem('currentUser');
                    this.router('home');
                }, 300);
            } else {
                this.renderCurrentPage();
            }
        }
        if (this.currentPage === 'admin') this.renderCurrentPage();
    }

    applyUserBan(payload) {
        const { userId, banned, by } = payload;
        const user = this.users[userId];
        if (!user) return;
        user.banned = banned;
        user.updatedAt = Date.now();
        this.saveLocalData();
        // 如果被禁用户当前在线，强制刷新
        if (this.currentUser && this.currentUser.id === userId) {
            this.logout();
            alert('你已被管理员封禁');
            this.router('home');
        }
        if (this.currentPage === 'admin') this.renderCurrentPage();
    }

    applyRoleChange(payload) {
        const { userId, role, by } = payload;
        const user = this.users[userId];
        if (!user) return;
        // 不允许通过 P2P 把别人提升为 owner
        if (role === 'owner') return;
        user.role = role;
        user.updatedAt = Date.now();
        this.saveLocalData();
        if (this.currentPage === 'admin') this.renderCurrentPage();
    }

    sendSyncResponse(targetId) {
        this.publish(`forum/sync/response/${targetId}`, {
            type: 'sync_summary',
            targetId: targetId,
            data: {
                postIds: this.posts.map(p => p.id),
                deletedPostIds: this.deletedPostIds,
                deletedCommentIds: this.deletedCommentIds,
                userCount: Object.keys(this.users).length
            }
        });
        Object.values(this.users).forEach((user, i) => {
            setTimeout(() => {
                if (user && user.id) {
                    this.publish(`forum/users/${user.id}`, { type: 'user', data: user }, true);
                }
            }, i * 50);
        });
        this.posts.forEach((post, i) => {
            setTimeout(() => {
                if (post && post.id) {
                    try {
                        const payload = JSON.stringify({ type: 'post', data: post });
                        if (payload.length <= 700 * 1024) {
                            this.publish(`forum/posts/${post.id}`, { type: 'post', data: post }, true);
                        } else {
                            const stateOnly = {
                                id: post.id,
                                isPinned: post.isPinned,
                                updatedAt: post.updatedAt,
                                _state_only: true,
                                title: post.title,
                                content: post.content,
                                authorId: post.authorId,
                                isAnonymous: post.isAnonymous,
                                isAnnouncement: post.isAnnouncement,
                                timestamp: post.timestamp
                            };
                            this.publish(`forum/posts/${post.id}`, { type: 'post_state', data: stateOnly }, true);
                        }
                    } catch (e) {}
                }
            }, i * 100);
        });
        this.friendRequests.forEach((fr, i) => {
            setTimeout(() => {
                if (fr && fr.id) {
                    const targetId = fr.from === this.currentUser?.id ? fr.to : fr.from;
                    if (targetId) {
                        this.publish(`forum/friends/${targetId}`, { type: 'friendRequest', data: fr }, true);
                    }
                }
            }, i * 50);
        });
    }

    applySyncData(data) {
        if (data.deletedPostIds && Array.isArray(data.deletedPostIds)) {
            data.deletedPostIds.forEach(id => {
                if (!this.deletedPostIds.includes(id)) {
                    this.deletedPostIds.push(id);
                }
            });
        }
        if (data.deletedCommentIds && Array.isArray(data.deletedCommentIds)) {
            data.deletedCommentIds.forEach(id => {
                if (!this.deletedCommentIds.includes(id)) {
                    this.deletedCommentIds.push(id);
                }
            });
        }
        if (data.posts) {
            data.posts.forEach(p => this.mergePost(p));
        }
        if (data.comments) {
            data.comments.forEach(c => this.mergeComment(c));
        }
        if (data.users) {
            Object.values(data.users).forEach(u => this.mergeUser(u));
        }
        if (data.postIds && Array.isArray(data.postIds)) {
            const missingIds = data.postIds.filter(id => !this.posts.find(p => p.id === id) && !this.deletedPostIds.includes(id));
            if (missingIds.length > 0) {
                console.log('缺少帖子ID:', missingIds);
            }
        }
        this.saveLocalData();
        if (this.currentPage === 'home' || this.currentPage === 'explore' || this.currentPage === 'profile') {
            this.renderCurrentPage();
        }
    }

    // ========== 路由系统 ==========
    router(page, params = {}) {
        const isSamePage = this.currentPage === page;
        const isPassiveRender = params && params._passiveRender;
        const main = document.getElementById('main-content');

        let savedInputs = {};
        let savedScroll = 0;
        let savedChatScroll = 0;
        if (isPassiveRender) {
            try {
                const titleEl = document.getElementById('post-title');
                const contentEl = document.getElementById('post-content');
                const commentEl = document.getElementById('comment-input');
                const chatInput = document.getElementById('chat-input');
                const searchEl = document.getElementById('search-input');
                const bioEl = document.getElementById('edit-bio');
                const nicknameEl = document.getElementById('edit-nickname');
                if (titleEl) savedInputs['post-title'] = titleEl.value;
                if (contentEl) savedInputs['post-content'] = contentEl.value;
                if (commentEl) savedInputs['comment-input'] = commentEl.value;
                if (chatInput) savedInputs['chat-input'] = chatInput.value;
                if (searchEl) savedInputs['search-input'] = searchEl.value;
                if (bioEl) savedInputs['edit-bio'] = bioEl.value;
                if (nicknameEl) savedInputs['edit-nickname'] = nicknameEl.value;
                savedScroll = window.scrollY || window.pageYOffset || 0;
                const msgArea = document.getElementById('chat-messages');
                if (msgArea) savedChatScroll = msgArea.scrollTop;
            } catch (e) {}
        }

        this.currentPage = page;
        if (params.postId) this.currentPostId = params.postId;
        if (params.friendId) this.currentFriendId = params.friendId;
        if (params.groupId) this.currentGroupId = params.groupId;
        document.querySelectorAll('.nav-btn, .nav-btn-mobile').forEach(btn => btn.classList.remove('active'));
        document.querySelectorAll(`[data-page="${page}"]`).forEach(btn => btn.classList.add('active'));

        const restrictedPages = ['home', 'explore', 'favorites', 'admin', 'profile', 'post', 'pending', 'rejected', 'chat', 'chats', 'group_chat', 'group_create'];
        if (restrictedPages.includes(page) && !this.currentUser) {
            main.innerHTML = this.renderLogin();
            return;
        }
        if (this.currentUser && this.users[this.currentUser.id]) {
            const fullUser = this.users[this.currentUser.id];
            if (fullUser.role === 'pending') {
                this.currentUser = null;
                localStorage.removeItem('currentUser');
                alert('⏳ 你的账号正在等待管理员审核。\n\n审核通过后才能访问。');
                main.innerHTML = this.renderLogin();
                return;
            }
            if (fullUser.role === 'rejected' || fullUser.banned) {
                this.currentUser = null;
                localStorage.removeItem('currentUser');
                alert('你的账号状态已变更，请重新登录。');
                main.innerHTML = this.renderLogin();
                return;
            }
        }
        switch(page) {
            case 'home': main.innerHTML = this.renderHome(); this.attachHomeEvents(); break;
            case 'explore': main.innerHTML = this.renderExplore(); break;
            case 'favorites': main.innerHTML = this.renderFavorites(); break;
            case 'login':
                main.innerHTML = this.renderLogin();
                if (!this.client || !this.connected) {
                    try {
                        this.connectMQTT(() => {
                            const statusEl = document.getElementById('login-status-text');
                            if (statusEl) statusEl.textContent = '✅ 服务器已连接，可以登录了';
                            const statusBox = document.getElementById('login-status');
                            if (statusBox) {
                                statusBox.style.background = '#f0fdf4';
                                statusBox.style.borderColor = '#86efac';
                                statusBox.style.color = '#16a34a';
                            }
                        });
                    } catch (e) {
                        const statusEl = document.getElementById('login-status-text');
                        if (statusEl) statusEl.textContent = '⚠️ 连接中...正在重试';
                    }
                    let loginCheckCount = 0;
                    const loginCheckInterval = setInterval(() => {
                        loginCheckCount++;
                        if (loginCheckCount > 20) {
                            clearInterval(loginCheckInterval);
                            return;
                        }
                        const statusEl = document.getElementById('login-status-text');
                        const statusBox = document.getElementById('login-status');
                        if (!statusEl || !statusBox) {
                            clearInterval(loginCheckInterval);
                            return;
                        }
                        if (this.connected) {
                            statusEl.textContent = '✅ 服务器已连接，可以登录了（已同步 ' + Object.keys(this.users).length + ' 个账号）';
                            statusBox.style.background = '#f0fdf4';
                            statusBox.style.borderColor = '#86efac';
                            statusBox.style.color = '#16a34a';
                            clearInterval(loginCheckInterval);
                        } else if (loginCheckCount > 3) {
                            statusEl.textContent = '⏳ 正在连接服务器并同步数据...（已等待 ' + loginCheckCount + ' 秒）';
                        }
                    }, 1000);
                } else {
                    const statusEl = document.getElementById('login-status-text');
                    const statusBox = document.getElementById('login-status');
                    if (statusEl && statusBox) {
                        statusEl.textContent = '✅ 服务器已连接，可以登录了（已同步 ' + Object.keys(this.users).length + ' 个账号）';
                        statusBox.style.background = '#f0fdf4';
                        statusBox.style.borderColor = '#86efac';
                        statusBox.style.color = '#16a34a';
                    }
                }
                break;
            case 'pending': main.innerHTML = this.renderPending(); break;
            case 'rejected': main.innerHTML = this.renderRejected(); break;
            case 'admin': main.innerHTML = this.renderAdmin(); break;
            case 'profile': main.innerHTML = this.renderProfile(params.userId); break;
            case 'post': main.innerHTML = this.renderPostDetail(params.postId); break;
            case 'chats': main.innerHTML = this.renderChatList(); break;
            case 'chat': main.innerHTML = this.renderChat(params.friendId || this.currentFriendId); 
                if (!isPassiveRender) {
                    setTimeout(() => {
                        const msgArea = document.getElementById('chat-messages');
                        if (msgArea) msgArea.scrollTop = msgArea.scrollHeight;
                        const input = document.getElementById('chat-input');
                        if (input) input.focus();
                    }, 50);
                }
                break;
            case 'group_chat':
                main.innerHTML = this.renderGroupChat(params.groupId);
                if (!isPassiveRender) {
                    setTimeout(() => {
                        const msgArea = document.getElementById('group-chat-messages');
                        if (msgArea) msgArea.scrollTop = msgArea.scrollHeight;
                        const input = document.getElementById('group-chat-input');
                        if (input) input.focus();
                    }, 50);
                }
                break;
            case 'group_create': main.innerHTML = this.renderGroupCreate(); break;
        }

        if (isPassiveRender) {
            try {
                Object.keys(savedInputs).forEach(id => {
                    const el = document.getElementById(id);
                    if (el && savedInputs[id] !== undefined) {
                        el.value = savedInputs[id];
                    }
                });
                if (savedScroll > 5) {
                    setTimeout(() => { window.scrollTo(0, savedScroll); }, 10);
                }
                const msgArea = document.getElementById('chat-messages');
                if (msgArea && savedChatScroll > 0) {
                    setTimeout(() => { msgArea.scrollTop = savedChatScroll; }, 10);
                }
            } catch (e) {}
        } else {
            window.scrollTo(0, 0);
        }
    }

    renderCurrentPage() {
        if (!this.currentPage) return;
        if (this._renderTimer) {
            clearTimeout(this._renderTimer);
        }
        this._renderTimer = setTimeout(() => {
            this.router(this.currentPage, { _passiveRender: true, postId: this.currentPostId, friendId: this.currentFriendId, groupId: this.currentGroupId });
            this.updateNotificationBadge();
        }, 300);
    }

    updateNotificationBadge() {
        if (!this.currentUser) return;
        const pendingRequests = this.friendRequests.filter(r => r.to === this.currentUser.id && r.status === 'pending').length;
        const chatBtn = document.querySelector('[data-page="chats"]');
        if (chatBtn) {
            let text = '💬 消息';
            if (pendingRequests > 0) {
                text = `💬 消息 <span style="background:#dc2626;color:white;font-size:0.7rem;padding:2px 6px;border-radius:10px;margin-left:4px;">${pendingRequests}</span>`;
            }
            chatBtn.innerHTML = text;
        }
    }

    // ========== 页面渲染 ==========
    renderHome() {
        const validPosts = this.posts.filter(p => !this.deletedPostIds.includes(p.id));
        const postList = [...validPosts].sort((a, b) => {
            const aAnnouncement = a.isAnnouncement ? 2 : 0;
            const bAnnouncement = b.isAnnouncement ? 2 : 0;
            const aPinned = a.isPinned ? 1 : 0;
            const bPinned = b.isPinned ? 1 : 0;
            const aScore = aAnnouncement + aPinned;
            const bScore = bAnnouncement + bPinned;
            if (aScore !== bScore) return bScore - aScore;
            return (b.timestamp || 0) - (a.timestamp || 0);
        });
        const connTip = !this.connected ? `<div class="config-tip" id="conn-tip">🟡 正在连接网络... 如果长时间未连接，请检查网络。<button onclick="app.connectMQTT()" class="btn btn-small btn-primary">重试</button></div>` : '';
        const fullUser = this.currentUser && this.users[this.currentUser.id];
        const isPending = fullUser && fullUser.role === 'pending';
        let pendingBanner = '';
        if (isPending) {
            pendingBanner = `
                <div class="audit-notice" style="margin:12px 0 20px 0;">
                    <div class="audit-notice-title">
                        <span class="audit-icon">⏳</span>
                        <strong>账号待审核</strong>
                    </div>
                    <div class="audit-notice-text">
                        你的注册申请已提交，正在等待管理员审核通过。<br>
                        保持页面打开，审核通过后会自动刷新。
                    </div>
                    <button onclick="app.resendAuditRequest()" class="btn btn-secondary" style="margin-top:12px;">📡 重新发送审核请求</button>
                </div>
            `;
        }

        return `
            <div class="home-page">
                <div class="hero-section">
                    <h1>🏫 学生校园墙</h1>
                    <p>校园交流社区，打开即共享</p>
                    ${this.currentUser && !isPending ? `
                        <button onclick="app.openPostModal()" class="btn btn-large btn-primary">✏️ 发布新帖</button>
                        ${(this.isAdmin(this.currentUser.id)) ? `<button onclick="app.openPostModal(true)" class="btn btn-large" style="background:#dc2626;color:white;margin-left:8px;">📢 发布公告</button>` : ''}
                    ` : isPending ? `<button class="btn btn-large" disabled style="cursor:not-allowed;opacity:0.6;">⏳ 审核通过后才能发帖</button>` : `<button onclick="app.router('login')" class="btn btn-large btn-primary">🔑 登录后发帖</button>`}
                </div>
                ${pendingBanner}
                ${connTip}
                <div class="posts-container">
                    <div class="posts-header">
                        <h2>📰 最新动态</h2>
                        <div class="filter-tabs">
                            <button class="tab-btn active">最新</button>
                        </div>
                    </div>
                    <div class="posts-list">
                        ${postList.length === 0 ? '<div class="empty-state">📝 还没有帖子，来做第一个发帖的人吧！<br><br>💡 提示：把文件夹发给朋友，朋友打开后你们就能互相看到内容了</div>' : ''}
                        ${postList.map(post => this.renderPostCard(post)).join('')}
                    </div>
                </div>
            </div>
        `;
    }

    renderPostCard(post) {
        const realAuthor = this.users[post.authorId] || { nickname: '未知用户', avatar: '👤' };
        const displayAuthor = post.isAnonymous ? { nickname: '🎭 匿名用户', avatar: '👤' } : realAuthor;
        const time = this.formatTime(post.timestamp);
        const hasImage = post.images && post.images.length > 0;
        const imageUrl = hasImage ? post.images[0] : null;
        const commentCount = this.comments.filter(c => c.postId === post.id && !this.deletedCommentIds.includes(c.id)).length;
        const isFav = this.favorites.includes(post.id);
        const isAdminUser = this.currentUser && this.isAdmin(this.currentUser.id);
        const isOwnerUser = this.currentUser && this.currentUser.id === post.authorId;
        const canManage = isAdminUser || isOwnerUser;
        let adminHtml = '';
        if (canManage) {
            adminHtml = '<div class="admin-actions">';
            if (isAdminUser) {
                adminHtml += `<button class="admin-action-btn announcement-btn" onclick="event.stopPropagation();app.toggleAnnouncement('${post.id}')">${post.isAnnouncement ? '📢 取消公告' : '📢 设为公告'}</button>`;
            }
            if (canManage) {
                adminHtml += `<button class="admin-action-btn" onclick="event.stopPropagation();app.togglePin('${post.id}')">${post.isPinned ? '📌 取消置顶' : '📌 置顶'}</button>`;
            }
            adminHtml += `<button class="admin-action-btn delete-btn" onclick="event.stopPropagation();app.deletePost('${post.id}')">🗑️ 删除</button>`;
            adminHtml += '</div>';
        }
        const authorOnclick = post.isAnonymous && !isAdminUser ? '' : `onclick="event.stopPropagation();app.router('profile', {userId: '${post.authorId}'})"`;
        const adminHint = (post.isAnonymous && isAdminUser) ? `<div style="font-size:0.75rem;color:#dc2626;margin-top:2px;">👁️ 管理员可见: 真实发布者 ${this.escapeHtml(realAuthor.nickname)}</div>` : '';
        return `
            <div class="post-card ${post.isAnnouncement ? 'post-card-announcement' : ''}" data-id="${post.id}">
                <div class="post-header">
                    <div class="post-author" ${authorOnclick} style="${post.isAnonymous && !isAdminUser ? 'cursor:default;' : ''}">
                        <div class="author-avatar">${this.renderAvatarContent(displayAuthor.avatar)}</div>
                        <div class="author-info">
                            <span class="author-name">${displayAuthor.nickname}</span>
                            <span class="post-time">${time}</span>
                            ${adminHint}
                        </div>
                    </div>
                    ${post.isAnnouncement ? '<span class="announcement-badge">📢 公告</span>' : ''}
                    ${post.isPinned && !post.isAnnouncement ? '<span class="pin-badge">📌 置顶</span>' : ''}
                    ${post.isAnonymous ? '<span class="anonymous-badge">🎭 匿名</span>' : ''}
                </div>
                <div class="post-body" onclick="app.router('post', {postId: '${post.id}'})">
                    <h3 class="post-title">${this.escapeHtml(post.title)}</h3>
                    <p class="post-content">${this.escapeHtml(post.content).substring(0, 200)}${post.content && post.content.length > 200 ? '...' : ''}</p>
                    ${hasImage ? `<div class="post-media-grid" onclick="event.stopPropagation();">${post.images.slice(0, 4).map((url, idx) => `<div class="post-media-item">${this.renderMedia(url)}</div>`).join('')}${post.images.length > 4 ? `<div class="post-media-item post-media-more">+${post.images.length - 4}</div>` : ''}</div>` : ''}
                </div>
                <div class="post-footer">
                    <button class="action-btn ${isFav ? 'active' : ''}" onclick="event.stopPropagation();app.toggleFavorite('${post.id}')">
                        ${isFav ? '⭐' : '☆'} ${isFav ? '已收藏' : '收藏'}
                    </button>
                    <button class="action-btn" onclick="event.stopPropagation();app.router('post', {postId: '${post.id}'})">
                        💬 ${commentCount} 评论
                    </button>
                    <button class="action-btn" onclick="event.stopPropagation();app.sharePost('${post.id}')">
                        📤 分享
                    </button>
                </div>
                ${adminHtml}
            </div>
        `;
    }

    renderExplore() {
        const validPosts = this.posts.filter(p => !this.deletedPostIds.includes(p.id));
        const allPosts = [...validPosts].sort((a, b) => {
            const aAnnouncement = a.isAnnouncement ? 2 : 0;
            const bAnnouncement = b.isAnnouncement ? 2 : 0;
            const aPinned = a.isPinned ? 1 : 0;
            const bPinned = b.isPinned ? 1 : 0;
            const aScore = aAnnouncement + aPinned;
            const bScore = bAnnouncement + bPinned;
            if (aScore !== bScore) return bScore - aScore;
            return (b.timestamp || 0) - (a.timestamp || 0);
        });
        const authors = [...new Set(allPosts.map(p => p.authorId).filter(id => id))];
        const q = this.currentSearchQuery.trim().toLowerCase();
        let searchResultsHtml = '';
        if (q) {
            const matchedPosts = allPosts.filter(p =>
                (p.title && p.title.toLowerCase().includes(q)) ||
                (p.content && p.content.toLowerCase().includes(q))
            );
            const matchedUserIds = Object.values(this.users).filter(u =>
                (u.nickname && u.nickname.toLowerCase().includes(q)) ||
                (u.username && u.username.toLowerCase().includes(q))
            ).map(u => u.id);
            const userMatchedPosts = allPosts.filter(p => matchedUserIds.includes(p.authorId));
            const allMatched = [...new Map([...matchedPosts, ...userMatchedPosts].map(p => [p.id, p])).values()].sort((a, b) => {
                const aAnn = a.isAnnouncement ? 2 : 0;
                const bAnn = b.isAnnouncement ? 2 : 0;
                const aPin = a.isPinned ? 1 : 0;
                const bPin = b.isPinned ? 1 : 0;
                if ((aAnn + aPin) !== (bAnn + bPin)) return (bAnn + bPin) - (aAnn + aPin);
                return (b.timestamp || 0) - (a.timestamp || 0);
            });
            if (allMatched.length > 0) {
                searchResultsHtml = `
                    <div class="search-results-section">
                        <div class="search-results-header">
                            <h2>🔍 搜索结果</h2>
                            <span class="search-count">找到 ${allMatched.length} 条相关内容 (关键词: "${this.escapeHtml(this.currentSearchQuery)}")</span>
                        </div>
                        <div class="posts-list">
                            ${allMatched.map(post => this.renderPostCard(post)).join('')}
                        </div>
                    </div>
                `;
            } else {
                searchResultsHtml = `
                    <div class="search-results-section">
                        <div class="search-results-header">
                            <h2>🔍 搜索结果</h2>
                            <span class="search-count">没有找到与 "${this.escapeHtml(this.currentSearchQuery)}" 相关的内容</span>
                        </div>
                        <div class="empty-state">
                            <div class="empty-icon">🔍</div>
                            <p>试试其他关键词吧</p>
                        </div>
                    </div>
                `;
            }
        }
        return `
            <div class="explore-page">
                <div class="search-section">
                    <input type="text" id="search-input" placeholder="🔍 搜索帖子标题、内容、作者..." value="${this.escapeHtml(this.currentSearchQuery)}" oninput="app.handleSearchInput(event)">
                </div>
                <div class="stats-section">
                    <div class="stat-card">
                        <div class="stat-number">${allPosts.length}</div>
                        <div class="stat-label">总帖子</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-number">${Object.keys(this.users).length}</div>
                        <div class="stat-label">注册用户</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-number">${this.comments.length}</div>
                        <div class="stat-label">总评论</div>
                    </div>
                </div>
                ${searchResultsHtml}
                ${!q ? `
                <div class="users-section">
                    <h2>👥 活跃用户</h2>
                    <div class="users-grid">
                        ${authors.slice(0, 20).map(id => {
                            const user = this.users[id] || { nickname: '未知用户', avatar: '👤' };
                            const userPosts = allPosts.filter(p => p.authorId === id).length;
                            return `
                                <div class="user-card" onclick="app.router('profile', {userId: '${id}'})">
                                    <div class="user-avatar-large">${this.renderAvatarContent(user.avatar)}</div>
                                    <div class="user-name">${user.nickname}</div>
                                    <div class="user-posts">${userPosts} 篇帖子</div>
                                </div>
                            `;
                        }).join('')}
                    </div>
                </div>
                ` : ''}
            </div>
        `;
    }

    renderFavorites() {
        const favPosts = this.favorites.map(id => this.posts.find(p => p.id === id)).filter(p => p);
        return `
            <div class="favorites-page">
                <h2>⭐ 我的收藏</h2>
                <div class="posts-list">
                    ${favPosts.length === 0 ? '<div class="empty-state">⭐ 还没有收藏任何帖子</div>' : ''}
                    ${favPosts.map(post => this.renderPostCard(post)).join('')}
                </div>
            </div>
        `;
    }

    renderLogin() {
        const hasOwner = this.getOwner();
        const pendingUsers = Object.values(this.users).filter(u => u.role === 'pending');
        const rejectedUsers = Object.values(this.users).filter(u => u.role === 'rejected');
        let pendingNotice = '';
        if (pendingUsers.length > 0) {
            pendingNotice = `
                <div class="audit-notice">
                    <div class="audit-notice-title">
                        <span class="audit-icon">⏳</span>
                        <strong>注册申请待审核</strong>
                    </div>
                    ${pendingUsers.map(u => `
                        <div class="audit-user-info">
                            <div>👤 <strong>${u.nickname}</strong> (@${u.username})</div>
                            <div style="font-size:0.85rem;color:#666;margin-top:4px;">
                                注册时间：${new Date(u.updatedAt).toLocaleString('zh-CN')}
                            </div>
                        </div>
                    `).join('')}
                    <div class="audit-notice-text">
                        你的注册申请已保存，正在等待管理员审核通过。<br>
                        请耐心等待，审核通过后即可用此账号登录。
                    </div>
                    <button onclick="app.resendAuditRequest()" class="btn btn-secondary" style="margin-top:12px;">📡 重新发送审核请求</button>
                    <button onclick="app.clearPendingUser()" class="btn btn-small" style="margin-top:8px;color:#999;">放弃此账号，重新注册</button>
                </div>
            `;
        }
        if (rejectedUsers.length > 0) {
            pendingNotice += `
                <div class="audit-notice" style="background:#fef2f2;border-color:#fecaca;">
                    <div class="audit-notice-title">
                        <span class="audit-icon">❌</span>
                        <strong>审核未通过</strong>
                    </div>
                    ${rejectedUsers.map(u => `
                        <div class="audit-user-info">
                            <div>👤 <strong>${u.nickname}</strong> (@${u.username})</div>
                            <div style="font-size:0.85rem;color:#666;margin-top:4px;">
                                注册时间：${new Date(u.updatedAt).toLocaleString('zh-CN')}
                            </div>
                        </div>
                    `).join('')}
                    <div class="audit-notice-text" style="color:#991b1b;">
                        你的注册申请未通过审核。
                    </div>
                    <button onclick="app.clearRejectedUser()" class="btn btn-small" style="margin-top:8px;color:#dc2626;">清除此记录，重新注册</button>
                </div>
            `;
        }
        return `
            <div class="auth-page">
                <div class="auth-container">
                    ${pendingNotice}
                    <div class="auth-tabs">
                        <button class="auth-tab active" onclick="app.switchAuthTab('login')">登录</button>
                        <button class="auth-tab" onclick="app.switchAuthTab('register')">注册</button>
                    </div>

                    <div class="backup-login-card" style="margin:12px 0;padding:16px;background:linear-gradient(135deg, #667eea 0%, #764ba2 100%);color:white;border-radius:12px;box-shadow:0 4px 12px rgba(102,126,234,0.3);">
                        <div style="font-weight:bold;font-size:1.05rem;margin-bottom:6px;">📱 换了新设备？用账号备份登录</div>
                        <div style="font-size:0.85rem;opacity:0.95;margin-bottom:12px;line-height:1.5;">
                            即使原设备关机、断网，也能用备份码直接登录。<br>
                            在原设备登录 → 我的主页 → 导出账号备份 → 复制到这里
                        </div>
                        <button onclick="app.toggleAccountImport()" class="btn btn-block" style="background:white;color:#667eea;border:none;font-weight:bold;padding:10px 16px;border-radius:8px;cursor:pointer;">
                            ${document.getElementById('account-import-form-display') === 'block' ? '收起' : '📋 点击这里输入备份码'}
                        </button>
                        <div id="account-import-form" style="display:none;margin-top:12px;padding:14px;background:rgba(255,255,255,0.95);border-radius:8px;color:#334155;">
                            <label style="display:block;font-size:0.85rem;color:#475569;margin-bottom:8px;font-weight:500;">👇 粘贴账号备份码</label>
                            <textarea id="account-import-code" rows="5" placeholder="粘贴从原设备复制的账号备份码..." style="width:100%;padding:10px;border:2px solid #cbd5e1;border-radius:8px;font-family:monospace;font-size:0.75rem;box-sizing:border-box;"></textarea>
                            <button onclick="app.importAccount()" class="btn btn-primary btn-block" style="margin-top:10px;background:#667eea;border:none;padding:12px;font-weight:bold;">✨ 导入并立即登录</button>
                            <button onclick="app.hideAccountImport()" class="btn btn-secondary btn-block" style="margin-top:6px;background:transparent;color:#64748b;border:1px solid #cbd5e1;">取消</button>
                        </div>
                    </div>

                    <div id="login-status" class="login-status" style="margin:12px 0;padding:12px 16px;background:#f0f5f9;border:1px solid #afc8da;border-radius:8px;font-size:0.9rem;color:#47709B;text-align:center;">
                        <span id="login-status-text">📡 正在连接服务器，请稍候...</span>
                    </div>
                    <div id="login-form">
                        <div style="text-align:center;color:#64748b;font-size:0.85rem;margin-bottom:8px;">— 或者用用户名密码登录 —</div>
                        <div class="form-group">
                            <label>用户名</label>
                            <input type="text" id="login-username" placeholder="输入用户名">
                        </div>
                        <div class="form-group">
                            <label>密码</label>
                            <input type="password" id="login-password" placeholder="输入密码">
                        </div>
                        <button onclick="app.login()" class="btn btn-primary btn-block">登录</button>
                        <div style="margin-top:10px;font-size:0.8rem;color:#94a3b8;text-align:center;line-height:1.5;">
                            💡 提示：如果登录失败，请使用上方的<span style="color:#667eea;font-weight:500;">账号备份</span>方式
                        </div>
                    </div>
                    <div id="register-form" style="display:none">
                        <div class="form-group">
                            <label>用户名</label>
                            <input type="text" id="reg-username" placeholder="设置用户名" maxlength="20">
                        </div>
                        <div class="form-group">
                            <label>昵称</label>
                            <input type="text" id="reg-nickname" placeholder="设置昵称" maxlength="20">
                        </div>
                        <div class="form-group">
                            <label>密码</label>
                            <input type="password" id="reg-password" placeholder="设置密码">
                        </div>
                        <div class="form-group">
                            <label>邀请码 <span class="hint">（普通用户留空，需管理员审核）</span></label>
                            <input type="text" id="reg-invite" placeholder="输入邀请码（可选）">
                        </div>
                        ${!hasOwner ? '<div class="info-tip">当前还没有总管理员，输入正确的所有者邀请码即可成为总管理员。</div>' : ''}
                        <button onclick="app.register()" class="btn btn-primary btn-block">注册</button>
                    </div>
                </div>
            </div>
        `;
    }

    renderPending() {
        return `
            <div class="auth-page">
                <div class="auth-container">
                    <h2>⏳ 等待审核</h2>
                    <p>你的注册申请已提交，正在等待管理员审核。</p>
                    <p>审核通过后即可发帖、评论和浏览完整内容。</p>
                    <button onclick="app.logout()" class="btn btn-secondary">退出登录</button>
                </div>
            </div>
        `;
    }

    renderRejected() {
        return `
            <div class="auth-page">
                <div class="auth-container">
                    <h2>❌ 审核未通过</h2>
                    <p>你的注册申请已被管理员拒绝。</p>
                    <p>如有疑问，请联系管理员。</p>
                    <button onclick="app.logout()" class="btn btn-secondary">退出登录</button>
                </div>
            </div>
        `;
    }

    renderAvatarContent(avatar) {
        if (avatar && (avatar.startsWith('data:image') || avatar.startsWith('http'))) {
            return `<img src="${avatar}" style="width:100%;height:100%;object-fit:cover;">`;
        }
        return avatar || '👤';
    }

    renderMedia(url) {
        if (url && url.startsWith('data:video/')) {
            return `<video src="${url}" controls muted preload="metadata" style="max-width:100%;max-height:400px;border-radius:var(--radius-sm);display:block;"></video>`;
        }
        return `<img src="${url}" alt="配图" onclick="app.showImageModal('${url}')" style="cursor:zoom-in;">`;
    }

    renderProfile(userId) {
        const user = this.users[userId] || { nickname: '未知用户', avatar: '👤', bio: '', theme: 'default', bgColor: '#f5f5f5' };
        const isMe = this.currentUser && this.currentUser.id === userId;
        const isAdmin = this.currentUser && this.isAdmin(this.currentUser.id);
        const isHidden = user.hideProfile && !isMe && !isAdmin;
        const userPosts = this.posts.filter(p => p.authorId === userId && !this.deletedPostIds.includes(p.id)).sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0));
        const themeClass = `theme-${user.theme || 'default'}`;

        if (isHidden) {
            return `
                <div class="profile-page ${themeClass}" style="--profile-bg: ${user.bgColor || '#f5f5f5'}">
                    <div class="profile-header" style="background: ${user.bgColor || '#f5f5f5'}">
                        <div class="profile-avatar">${this.renderAvatarContent(user.avatar)}</div>
                        <div class="profile-info">
                            <h2>${user.nickname}</h2>
                            <p class="profile-bio">🔒 该用户已隐藏个人资料</p>
                            <div class="profile-stats">
                                <span>📝 ${userPosts.length} 帖子</span>
                            </div>
                        </div>
                        ${this.renderProfileActionBtn(userId)}
                    </div>
                    <div class="profile-posts">
                        <div class="empty-state" style="padding:60px 20px;">
                            <div style="font-size:3rem;margin-bottom:12px;">🔒</div>
                            <h3>该用户已隐藏个人资料</h3>
                            <p style="color:#666;margin-top:8px;">无法查看发布的帖子</p>
                        </div>
                    </div>
                </div>
            `;
        }

        return `
            <div class="profile-page ${themeClass}" style="--profile-bg: ${user.bgColor || '#f5f5f5'}">
                <div class="profile-header" style="background: ${user.bgColor || '#f5f5f5'}">
                    <div class="profile-avatar">${this.renderAvatarContent(user.avatar)}</div>
                    <div class="profile-info">
                        <h2>${user.nickname}</h2>
                        <p class="profile-bio">${user.bio || '这个人很懒，什么都没写~'}</p>
                        <div class="profile-stats">
                            <span>📝 ${userPosts.length} 帖子</span>
                            ${user.hideProfile ? '<span style="margin-left:12px;color:#f59e0b;">🔒 已隐藏</span>' : ''}
                        </div>
                    </div>
                    ${isMe ? `
                        <div style="display:flex;gap:8px;align-items:center;">
                            <button onclick="app.openProfileEdit()" class="btn btn-secondary">✏️ 编辑主页</button>
                            <button onclick="app.exportAccount()" class="btn btn-secondary" style="background:#e0e7ff;color:#3730a3;">📋 导出账号备份</button>
                        </div>` : this.renderProfileActionBtn(userId)}
                </div>
                <div class="profile-posts">
                    <h3>📰 发布的帖子</h3>
                    <div class="posts-list">
                        ${userPosts.length === 0 ? '<div class="empty-state">还没有发布过帖子</div>' : ''}
                        ${userPosts.map(post => this.renderPostCard(post)).join('')}
                    </div>
                </div>
            </div>
        `;
    }

    renderProfileActionBtn(userId) {
        if (!this.currentUser) return '';
        const status = this.getFriendRequestStatus(userId);
        if (status === 'accepted') {
            return `<button onclick="app.router('chat', { friendId: '${userId}' })" class="btn btn-primary">💬 私信</button>`;
        }
        if (status === 'waiting') {
            return `<button class="btn btn-secondary" disabled style="cursor:not-allowed;opacity:0.7;">⏳ 等待对方同意</button>`;
        }
        if (status === 'received') {
            return `
                <button onclick="app.acceptFriendRequest('${userId}')" class="btn btn-primary" style="margin-right:8px;">✅ 同意私聊</button>
                <button onclick="app.rejectFriendRequest('${userId}')" class="btn btn-secondary">❌ 拒绝</button>
            `;
        }
        if (status === 'rejected') {
            return `<button onclick="app.sendFriendRequest('${userId}')" class="btn btn-primary">📨 重新发送私聊申请</button>`;
        }
        return `<button onclick="app.sendFriendRequest('${userId}')" class="btn btn-primary">📨 发送私聊申请</button>`;
    }

    renderPostDetail(postId) {
        if (this.deletedPostIds.includes(postId)) {
            return `
                <div class="post-detail-page">
                    <button onclick="app.router('home')" class="back-btn">← 返回</button>
                    <div class="empty-state" style="padding:60px 20px;">
                        <div style="font-size:3rem;margin-bottom:12px;">🗑️</div>
                        <h3>帖子已被删除</h3>
                        <p style="color:#666;margin-top:8px;">无法查看该帖子</p>
                    </div>
                </div>
            `;
        }
        const post = this.posts.find(p => p.id === postId);
        if (!post) return '<div class="empty-state">帖子不存在或已被删除</div>';

        const isAdminUser = this.currentUser && this.isAdmin(this.currentUser.id);
        const realAuthor = this.users[post.authorId] || { nickname: '未知用户', avatar: '👤' };
        const author = post.isAnonymous ? { nickname: '🎭 匿名用户', avatar: '👤' } : realAuthor;
        const time = this.formatTime(post.timestamp);
        const postComments = this.comments.filter(c => c.postId === postId && !this.deletedCommentIds.includes(c.id)).sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0));
        const isFav = this.favorites.includes(postId);

        const authorOnclick = post.isAnonymous && !isAdminUser ? '' : `onclick="app.router('profile', {userId: '${post.authorId}'})"`;
        const adminHint = (post.isAnonymous && isAdminUser) ? `<div style="font-size:0.8rem;color:#dc2626;margin-top:4px;">👁️ 管理员可见: 真实发布者 ${this.escapeHtml(realAuthor.nickname)} (${post.authorId})</div>` : '';
        return `
            <div class="post-detail-page">
                <button onclick="app.router('home')" class="back-btn">← 返回</button>
                <div class="post-detail">
                    <div class="post-header">
                        <div class="post-author" ${authorOnclick} style="${post.isAnonymous && !isAdminUser ? 'cursor:default;' : ''}">
                            <div class="author-avatar">${this.renderAvatarContent(author.avatar)}</div>
                            <div class="author-info">
                                <span class="author-name">${author.nickname}</span>
                                <span class="post-time">${time}</span>
                                ${adminHint}
                            </div>
                        </div>
                        ${post.isAnnouncement ? '<span class="announcement-badge">📢 公告</span>' : ''}
                        ${post.isPinned && !post.isAnnouncement ? '<span class="pin-badge">📌 置顶</span>' : ''}
                        ${post.isAnonymous ? '<span class="anonymous-badge">🎭 匿名</span>' : ''}
                    </div>
                    <h1 class="post-detail-title">${this.escapeHtml(post.title)}</h1>
                    <div class="post-detail-content">${this.escapeHtml(post.content).replace(/\n/g, '<br>')}</div>
                    ${post.images && post.images.length ? `
                        <div class="post-files">
                            ${post.images.map(url => `<div class="file-item image">${this.renderMedia(url)}</div>`).join('')}
                        </div>
                    ` : ''}
                    <div class="post-actions">
                        ${(this.currentUser && (this.currentUser.id === post.authorId || this.isAdmin(this.currentUser.id))) ? `
                            <button class="action-btn ${post.isPinned ? 'active' : ''}" onclick="app.togglePin('${postId}')">
                                ${post.isPinned ? '📌 取消置顶' : '📌 置顶'}
                            </button>
                        ` : ''}
                        ${(this.currentUser && this.isAdmin(this.currentUser.id)) ? `
                            <button class="action-btn ${post.isAnnouncement ? 'active' : ''}" onclick="app.toggleAnnouncement('${postId}')">
                                ${post.isAnnouncement ? '📢 取消公告' : '📢 设为公告'}
                            </button>
                        ` : ''}
                        ${(this.currentUser && (this.currentUser.id === post.authorId || this.isAdmin(this.currentUser.id))) ? `
                            <button class="action-btn" onclick="app.deletePost('${postId}')" style="color:#dc2626;border-color:#fecaca;">
                                🗑️ 删除
                            </button>
                        ` : ''}
                        <button class="action-btn ${isFav ? 'active' : ''}" onclick="app.toggleFavorite('${postId}')">
                            ${isFav ? '⭐ 已收藏' : '☆ 收藏'}
                        </button>
                        <button class="action-btn" onclick="app.sharePost('${postId}')">📤 分享</button>
                    </div>
                </div>
                <div class="comments-section">
                    <h3>💬 评论 (${postComments.length})</h3>
                    ${this.currentUser && (!this.users[this.currentUser.id] || this.users[this.currentUser.id].role !== 'pending') ? `
                        <div class="comment-form">
                            <textarea id="comment-content" placeholder="写下你的评论..." rows="3"></textarea>
                            <button onclick="app.submitComment('${postId}')" class="btn btn-primary">发表评论</button>
                        </div>
                    ` : this.currentUser ? '<p class="login-tip">⏳ 审核通过后即可评论</p>' : '<p class="login-tip">🔑 登录后即可评论</p>'}
                    <div class="comments-list">
                        ${postComments.length === 0 ? '<div class="empty-state">还没有评论，来说两句吧~</div>' : ''}
                        ${postComments.map(c => {
                            const cAuthor = this.users[c.authorId] || { nickname: '未知用户', avatar: '👤' };
                            const canDelete = this.currentUser && (this.currentUser.id === c.authorId || this.isAdmin(this.currentUser.id));
                            return `
                                <div class="comment-item">
                                    <div class="comment-author">
                                        <span class="author-avatar-small">${this.renderAvatarContent(cAuthor.avatar)}</span>
                                        <span class="author-name">${cAuthor.nickname}</span>
                                        <span class="comment-time">${this.formatTime(c.timestamp)}</span>
                                        ${canDelete ? `<button class="admin-action-btn delete-btn" onclick="event.stopPropagation();app.deleteComment('${c.id}')" style="margin-left:8px;">🗑️ 删除</button>` : ''}
                                    </div>
                                    <div class="comment-content">${this.escapeHtml(c.content)}</div>
                                </div>
                            `;
                        }).join('')}
                    </div>
                </div>
            </div>
        `;
    }

    // ========== 用户系统 ==========
    checkLoginStatus() {
        try {
            const saved = localStorage.getItem('currentUser');
            if (saved) {
                const savedUser = JSON.parse(saved);
                if (this.users[savedUser.id]) {
                    const fullUser = this.users[savedUser.id];
                    this.currentUser = { id: savedUser.id, username: fullUser.username, nickname: fullUser.nickname };
                    if (fullUser.role === 'pending' || fullUser.role === 'rejected' || fullUser.banned) {
                        console.log('登录状态检测到受限用户，允许连接 MQTT 等待同步状态:', fullUser.role, fullUser.banned ? '(banned)' : '');
                    }
                } else {
                    this.currentUser = savedUser;
                }
                this.updateUserUI();
            }
        } catch (e) {
            console.warn('登录状态恢复失败:', e);
            localStorage.removeItem('currentUser');
        }
    }

    updateUserUI() {
        const section = document.getElementById('user-section');
        if (!section) return;
        if (this.currentUser) {
            const user = this.users[this.currentUser.id] || this.currentUser;
            section.innerHTML = `
                <div class="user-menu">
                    <span class="user-greeting" onclick="app.toggleUserMenu()">
                        <span class="user-avatar-small">${this.renderAvatarContent(user.avatar)}</span>
                        ${user.nickname} ${this.isOwner(this.currentUser.id) ? '👑' : (this.isAdmin(this.currentUser.id) ? '🔧' : '')}
                    </span>
                    <div class="dropdown-menu" id="user-dropdown">
                        <button onclick="app.router('profile', {userId: '${this.currentUser.id}'})">🏠 我的主页</button>
                        <button onclick="app.openProfileEdit()">✏️ 编辑资料</button>
                        ${this.isAdmin(this.currentUser.id) ? `<button onclick="app.router('admin')">🛡️ 审核中心</button>` : ''}
                        <button onclick="app.logout()">🚪 退出登录</button>
                    </div>
                </div>
            `;
        } else {
            section.innerHTML = `<button onclick="app.router('login')" class="btn btn-primary">登录 / 注册</button>`;
        }
        this.updateNotificationBadge();
    }

    toggleUserMenu() {
        const dropdown = document.getElementById('user-dropdown');
        if (dropdown) dropdown.classList.toggle('show');
    }

    resendAuditRequest() {
        const pendingUsers = Object.values(this.users).filter(u => u.role === 'pending');
        if (pendingUsers.length === 0) {
            alert('没有待审核的注册申请');
            return;
        }
        try {
            if (!this.connected) {
                this.connectMQTT(() => {
                    pendingUsers.forEach((user, idx) => {
                        setTimeout(() => {
                            user.updatedAt = Date.now();
                            this.users[user.id] = user;
                            this.saveLocalData();
                            this.publish(`forum/users/${user.id}`, { type: 'user', data: user }, true);
                            this.publish('forum/audit/requests', {
                                type: 'audit_request',
                                userId: user.id,
                                nickname: user.nickname,
                                username: user.username,
                                timestamp: Date.now()
                            }, true);
                        }, idx * 500);
                    });
                    setTimeout(() => {
                        alert('✅ 审核请求已重新发送！\n\n请等待管理员审核。\n\n你可以关闭此页面，稍后回来查看状态。');
                    }, pendingUsers.length * 500 + 500);
                });
            } else {
                pendingUsers.forEach((user, idx) => {
                    setTimeout(() => {
                        user.updatedAt = Date.now();
                        this.users[user.id] = user;
                        this.saveLocalData();
                        this.publish(`forum/users/${user.id}`, { type: 'user', data: user }, true);
                        this.publish('forum/audit/requests', {
                            type: 'audit_request',
                            userId: user.id,
                            nickname: user.nickname,
                            username: user.username,
                            timestamp: Date.now()
                        }, true);
                    }, idx * 500);
                });
                setTimeout(() => {
                    alert('✅ 审核请求已重新发送！\n\n请等待管理员审核。');
                }, pendingUsers.length * 500 + 200);
            }
        } catch (e) {
            console.error('重新发送审核请求失败:', e);
            alert('❌ 发送失败: ' + e.message + '\n\n请刷新页面后重试。');
        }
    }

    clearPendingUser() {
        if (!confirm('确定要放弃此注册申请吗？\n放弃后需要重新注册。')) return;
        Object.values(this.users).forEach(u => {
            if (u.role === 'pending') {
                delete this.users[u.id];
            }
        });
        this.saveLocalData();
        this.router('login');
    }

    clearRejectedUser() {
        Object.values(this.users).forEach(u => {
            if (u.role === 'rejected') {
                delete this.users[u.id];
            }
        });
        this.saveLocalData();
        this.router('login');
    }

    switchAuthTab(tab) {
        document.querySelectorAll('.auth-tab').forEach(t => t.classList.remove('active'));
        event.target.classList.add('active');
        document.getElementById('login-form').style.display = tab === 'login' ? 'block' : 'none';
        document.getElementById('register-form').style.display = tab === 'register' ? 'block' : 'none';
    }

    register() {
        const username = document.getElementById('reg-username').value.trim();
        const nickname = document.getElementById('reg-nickname').value.trim() || username;
        const password = document.getElementById('reg-password').value;
        const inviteCode = document.getElementById('reg-invite').value.trim();

        if (!username || !password) return alert('请填写完整信息');
        if (password.length < 4) return alert('密码至少4位');
        if (Object.values(this.users).some(u => u.username === username)) return alert('用户名已存在');

        const userId = 'user_' + Date.now();
        let role = 'pending';

        // 判断邀请码
        if (inviteCode === OWNER_CODE) {
            // 如果本地已有 owner，只能注册为 admin
            const existingOwner = this.getOwner();
            role = existingOwner ? 'admin' : 'owner';
        } else if (inviteCode === ADMIN_CODE) {
            role = 'admin';
        }

        const userData = {
            id: userId,
            username,
            nickname,
            password,
            role,
            banned: false,
            avatar: '👤',
            bio: '',
            theme: 'default',
            bgColor: '#f5f5f5',
            approvedBy: role === 'owner' || role === 'admin' ? 'system' : null,
            approvedAt: role === 'owner' || role === 'admin' ? Date.now() : null,
            updatedAt: Date.now()
        };

        this.users[userId] = userData;
        this.saveLocalData();

        if (role === 'pending') {
            try {
                this.connectMQTT(() => {
                    this.publish(`forum/users/${userId}`, { type: 'user', data: userData }, true);
                    this.publish('forum/audit/requests', {
                        type: 'audit_request',
                        userId,
                        nickname,
                        username,
                        timestamp: Date.now()
                    }, true);
                });
            } catch (e) {
                console.log('注册时 MQTT 连接失败:', e);
            }
            alert('✅ 注册申请已提交！\n\n请等待管理员审核通过。\n\n你可以关闭此页面，稍后回来查看状态。\n审核通过后即可登录。');
            this.router('login');
        } else {
            this.currentUser = { id: userId, username, nickname };
            localStorage.setItem('currentUser', JSON.stringify(this.currentUser));
            this.updateUserUI();
            this.connectMQTT();
            setTimeout(() => {
                this.publish(`forum/users/${userId}`, { type: 'user', data: userData }, true);
            }, 1500);
            alert(`🎉 注册成功！身份：${role === 'owner' ? '总管理员' : '管理员'}`);
            this.router('home');
        }
    }

    disconnectMQTT() {
        try {
            if (this.client) {
                this.client.end(true);
                this.client = null;
            }
        } catch (e) {
            console.log('MQTT 断开异常:', e);
        }
        this.connected = false;
    }

    login() {
        const username = document.getElementById('login-username').value.trim();
        const password = document.getElementById('login-password').value;

        if (!username || !password) return alert('请填写完整信息');

        const tryLogin = () => {
            const user = Object.values(this.users).find(u => u.username === username && u.password === password);
            if (!user) return false;

            if (user.role === 'pending') {
                alert('⏳ 你的账号正在等待管理员审核。\n\n审核通过后才能登录。\n请耐心等待，或使用其他账号。');
                return true;
            }

            if (user.role === 'rejected') {
                alert('❌ 你的注册申请未通过审核，无法登录。');
                return true;
            }

            if (user.banned) {
                alert('你已被管理员封禁，无法登录。');
                return true;
            }

            this.currentUser = { id: user.id, username: user.username, nickname: user.nickname };
            localStorage.setItem('currentUser', JSON.stringify(this.currentUser));
            this.updateUserUI();

            try {
                this.connectMQTT(() => {
                    this.publish(`forum/users/${user.id}`, { type: 'user', data: this.users[user.id] }, true);
                });
            } catch (e) {
                console.error('登录后 MQTT 连接失败:', e);
            }

            alert('✅ 登录成功！欢迎回来，' + (user.nickname || user.username));
            this.router('home');
            return true;
        };

        if (tryLogin()) return;

        alert('⏳ 正在从服务器同步账号数据...\n\n请稍候。\n\n💡 如果多次失败，请使用页面上方的"账号备份登录"功能。');

        if (!this.client || !this.connected) {
            try {
                this.connectMQTT();
            } catch (e) {
                console.error('登录时 MQTT 连接失败:', e);
            }
        }

        let attempts = 0;
        const maxAttempts = 8;
        const retryInterval = setInterval(() => {
            attempts++;
            if (attempts >= maxAttempts) {
                clearInterval(retryInterval);
                const user = Object.values(this.users).find(u => u.username === username);
                if (user) {
                    alert('❌ 密码错误！请重新输入。\n\n💡 忘记密码？请使用原设备登录后重置，或使用账号备份码登录。');
                } else {
                    const showBackup = confirm('❌ 没有找到这个账号！\n\n可能原因：\n1. 用户名输入错误\n2. 原设备已经很久没上线了，账号数据没有保存在服务器\n3. MQTT 服务器暂时不可用\n\n建议：使用页面上方的"📱 账号备份登录"功能\n\n点击"确定"按钮立即切换到备份登录方式');
                    if (showBackup) {
                        const form = document.getElementById('account-import-form');
                        if (form) {
                            form.style.display = 'block';
                            form.scrollIntoView({ behavior: 'smooth', block: 'center' });
                        }
                    }
                }
                return;
            }
            if (tryLogin()) {
                clearInterval(retryInterval);
            } else {
                console.log('🔄 登录重试 ' + attempts + '/' + maxAttempts + ', 当前用户数: ' + Object.keys(this.users).length);
            }
        }, 1500);
    }

    toggleAccountImport() {
        const form = document.getElementById('account-import-form');
        if (form) {
            form.style.display = form.style.display === 'block' ? 'none' : 'block';
        }
    }

    showAccountImport() {
        const form = document.getElementById('account-import-form');
        if (form) form.style.display = 'block';
    }

    hideAccountImport() {
        const form = document.getElementById('account-import-form');
        if (form) form.style.display = 'none';
    }

    exportAccount() {
        if (!this.currentUser) {
            alert('请先登录');
            return;
        }
        const user = this.users[this.currentUser.id];
        if (!user) {
            alert('找不到用户数据');
            return;
        }
        const exportData = {
            v: 1,
            id: user.id,
            username: user.username,
            nickname: user.nickname,
            password: user.password,
            role: user.role,
            avatar: user.avatar,
            bio: user.bio,
            theme: user.theme,
            bgColor: user.bgColor,
            approvedBy: user.approvedBy,
            approvedAt: user.approvedAt,
            updatedAt: Date.now()
        };
        try {
            const jsonStr = JSON.stringify(exportData);
            const b64 = btoa(unescape(encodeURIComponent(jsonStr)));
            const header = '=== XFA ACCOUNT BACKUP ===\n';
            const footer = '\n=== END ===';
            const formatted = header + b64.match(/.{1,60}/g).join('\n') + footer;

            const exportCard = document.createElement('div');
            exportCard.id = 'account-export-modal';
            exportCard.style.cssText = 'position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.7);display:flex;align-items:center;justify-content:center;z-index:10000;padding:16px;';
            exportCard.innerHTML = `
                <div style="background:white;border-radius:16px;padding:20px;max-width:500px;width:100%;max-height:90vh;overflow-y:auto;">
                    <div style="font-size:1.1rem;font-weight:bold;margin-bottom:8px;color:#334155;">📋 你的账号备份码</div>
                    <div style="font-size:0.85rem;color:#64748b;margin-bottom:12px;line-height:1.6;">
                        请完整复制下方文字，发送到新设备粘贴即可登录。<br>
                        <span style="color:#dc2626;">⚠️ 不要发给其他人！包含密码</span>
                    </div>
                    <textarea id="backup-code-display" readonly style="width:100%;padding:10px;border:2px solid #cbd5e1;border-radius:8px;font-family:monospace;font-size:0.7rem;min-height:200px;box-sizing:border-box;">${formatted}</textarea>
                    <div style="display:flex;gap:8px;margin-top:12px;">
                        <button onclick="app.copyBackupCode()" style="flex:1;padding:12px;background:#667eea;color:white;border:none;border-radius:8px;font-weight:bold;cursor:pointer;font-size:0.9rem;">📋 一键复制</button>
                        <button onclick="app.closeExportModal()" style="padding:12px 20px;background:#f1f5f9;color:#334155;border:none;border-radius:8px;cursor:pointer;font-size:0.9rem;">关闭</button>
                    </div>
                </div>
            `;
            document.body.appendChild(exportCard);
        } catch (e) {
            console.error('导出账号失败:', e);
            alert('导出失败: ' + e.message);
        }
    }

    copyBackupCode() {
        const textarea = document.getElementById('backup-code-display');
        if (!textarea) return;
        textarea.select();
        textarea.setSelectionRange(0, textarea.value.length);
        try {
            document.execCommand('copy');
            alert('✅ 已复制到剪贴板！\n\n现在可以发送到新设备粘贴登录。');
        } catch (e) {
            alert('自动复制失败，请手动长按选择全部文字复制。');
        }
    }

    closeExportModal() {
        const modal = document.getElementById('account-export-modal');
        if (modal) modal.remove();
    }

    importAccount() {
        const codeInput = document.getElementById('account-import-code');
        if (!codeInput) return;
        const rawCode = codeInput.value.trim();
        if (!rawCode) {
            alert('请粘贴账号备份码');
            return;
        }
        try {
            let b64 = rawCode
                .replace(/=*=* XFA ACCOUNT BACKUP =*=*/g, '')
                .replace(/=*=* END =*=*/g, '')
                .replace(/\s+/g, '');
            const jsonStr = decodeURIComponent(escape(atob(b64)));
            const data = JSON.parse(jsonStr);
            if (!data || !data.id || !data.username || !data.password) {
                alert('备份码格式错误，请确认内容完整');
                return;
            }
            const existingUser = this.users[data.id];
            if (existingUser) {
                if (!confirm('本设备已有此账号数据，是否用备份中的数据覆盖？')) {
                    return;
                }
            }
            const userData = {
                id: data.id,
                username: data.username,
                nickname: data.nickname,
                password: data.password,
                role: data.role || 'user',
                banned: false,
                avatar: data.avatar || '👤',
                bio: data.bio || '',
                theme: data.theme || 'default',
                bgColor: data.bgColor || '#f5f5f5',
                approvedBy: data.approvedBy || null,
                approvedAt: data.approvedAt || null,
                hideProfile: data.hideProfile || false,
                updatedAt: Date.now()
            };
            this.users[data.id] = userData;
            this.saveLocalData();
            this.currentUser = { id: data.id, username: data.username, nickname: data.nickname };
            localStorage.setItem('currentUser', JSON.stringify(this.currentUser));
            try {
                this.connectMQTT(() => {
                    this.publish(`forum/users/${data.id}`, { type: 'user', data: userData }, true);
                });
            } catch (e) {
                console.error('连接 MQTT 失败:', e);
            }
            setTimeout(() => {
                this.updateUserUI();
                alert('✅ 账号导入成功！欢迎回来，' + data.nickname);
                this.router('home');
            }, 1000);
        } catch (e) {
            console.error('导入失败:', e);
            alert('❌ 导入失败：备份码格式不正确或已损坏\n\n请确保复制了完整的备份码内容。');
        }
    }

    logout() {
        this.currentUser = null;
        localStorage.removeItem('currentUser');
        this.saveLocalData();
        this.updateUserUI();
        this.router('home');
    }

    // ========== 管理员后台 ==========
    renderAdmin() {
        if (!this.currentUser || !this.isAdmin(this.currentUser.id)) {
            return '<div class="empty-state">🚫 你没有权限访问此页面</div>';
        }
        const isOwner = this.isOwner(this.currentUser.id);
        const isAdmin = this.isAdmin(this.currentUser.id);
        const pendingUsers = Object.values(this.users).filter(u => u.role === 'pending');
        const rejectedUsers = Object.values(this.users).filter(u => u.role === 'rejected');
        const activeUsers = Object.values(this.users).filter(u => u.id !== this.currentUser.id && u.role !== 'pending' && u.role !== 'rejected');
        const allUsers = Object.values(this.users).filter(u => u.id !== this.currentUser.id);

        return `
            <div class="admin-page">
                <h2>🛡️ 审核中心</h2>
                <div class="admin-toolbar">
                    <button onclick="app.queryPendingUsers()" class="btn btn-success">🔍 查询待审核用户</button>
                    <button onclick="app.refreshUserData()" class="btn btn-secondary">🔄 重新发布所有用户数据</button>
                    <button onclick="app.requestSyncFromOthers()" class="btn btn-secondary">📡 请求其他用户同步数据</button>
                </div>
                <div class="admin-status">
                    <div>当前连接：${this.connected ? '🟢 已连接' : '🔴 未连接'}</div>
                    <div>本地用户总数：${Object.keys(this.users).length}</div>
                </div>

                <div class="admin-section">
                    <h3>⏳ 待审核用户 (${pendingUsers.length})</h3>
                    ${pendingUsers.length === 0 ? '<div class="empty-state">暂时没有待审核用户\n\n如果确认有人注册但没有显示，请点击上方"查询待审核用户"按钮\n\n如果仍看不到，请让对方：\n1. 打开或刷新登录页面\n2. 点击页面上方的"📡 重新发送审核请求"按钮</div>' : `
                        <div class="admin-list">
                            ${pendingUsers.map(u => `
                                <div class="admin-item">
                                    <div class="admin-info">
                                        <span class="admin-avatar">${this.renderAvatarContent(u.avatar)}</span>
                                        <div>
                                            <div>${u.nickname} (@${u.username})</div>
                                            <div class="role-label">注册时间：${new Date(u.updatedAt).toLocaleString('zh-CN')}</div>
                                        </div>
                                    </div>
                                    <div class="admin-actions">
                                        <button onclick="app.approveUser('${u.id}')" class="btn btn-success">✅ 通过</button>
                                        <button onclick="app.rejectUser('${u.id}')" class="btn btn-danger">❌ 拒绝</button>
                                    </div>
                                </div>
                            `).join('')}
                        </div>
                    `}
                </div>

                ${rejectedUsers.length > 0 ? `
                <div class="admin-section">
                    <h3>❌ 已拒绝用户 (${rejectedUsers.length})</h3>
                    <div class="admin-list">
                        ${rejectedUsers.map(u => `
                            <div class="admin-item">
                                <div class="admin-info">
                                    <span class="admin-avatar">${this.renderAvatarContent(u.avatar)}</span>
                                    <div>
                                        <div>${u.nickname} (@${u.username})</div>
                                        <div class="role-label">拒绝时间：${new Date(u.approvedAt || u.updatedAt).toLocaleString('zh-CN')}</div>
                                    </div>
                                </div>
                                <div class="admin-actions">
                                    <button onclick="app.approveUser('${u.id}')" class="btn btn-small btn-success">改为通过</button>
                                </div>
                            </div>
                        `).join('')}
                    </div>
                </div>
                ` : ''}

                <div class="admin-section">
                    <h3>👥 活跃用户 (${activeUsers.length})</h3>
                    <div class="admin-list">
                        ${activeUsers.map(u => {
                            const roleLabel = u.role === 'owner' ? '👑 总管理员' : (u.role === 'admin' ? '🔧 管理员' : '👤 普通用户');
                            const bannedLabel = u.banned ? '<span class="banned-label">🚫 已封禁</span>' : '';
                            return `
                                <div class="admin-item ${u.banned ? 'banned' : ''}">
                                    <div class="admin-info">
                                        <span class="admin-avatar">${this.renderAvatarContent(u.avatar)}</span>
                                        <div>
                                            <div>${u.nickname} (@${u.username}) ${bannedLabel}</div>
                                            <div class="role-label">${roleLabel}</div>
                                        </div>
                                    </div>
                                    <div class="admin-actions">
                                        ${isAdmin && u.role === 'user' ? `<button onclick="app.grantAdmin('${u.id}')" class="btn btn-small btn-primary">🔧 设为管理员</button>` : ''}
                                        ${isAdmin && u.role === 'admin' ? `<button onclick="app.revokeAdmin('${u.id}')" class="btn btn-small btn-secondary">撤销管理员</button>` : ''}
                                        ${!u.banned && u.role !== 'owner' ? `<button onclick="app.banUser('${u.id}')" class="btn btn-small btn-danger">封禁</button>` : ''}
                                        ${u.banned ? `<button onclick="app.unbanUser('${u.id}')" class="btn btn-small btn-success">解封</button>` : ''}
                                    </div>
                                </div>
                            `;
                        }).join('')}
                    </div>
                </div>
            </div>
        `;
    }

    refreshUserData() {
        if (!this.connected) {
            alert('⚠️ 当前未连接到网络，请检查网络连接后重试');
            return;
        }
        try {
            Object.values(this.users).forEach(user => {
                this.publish(`forum/users/${user.id}`, { type: 'user', data: user }, true);
            });
            this.requestSync();
            alert('✅ 已重新发布用户数据并请求同步。\n\n请等待几秒后页面会自动刷新。\n\n如果仍看不到待审核用户，请让对方重新打开一次网页（这样他们的注册信息会被重新发送）。');
            setTimeout(() => {
                this.renderCurrentPage();
            }, 3000);
            setTimeout(() => {
                this.renderCurrentPage();
            }, 6000);
        } catch (e) {
            console.error('刷新用户数据失败:', e);
            alert('❌ 刷新失败: ' + e.message);
        }
    }

    queryPendingUsers() {
        if (!this.connected) {
            alert('⚠️ 当前未连接到网络，请检查网络连接后重试');
            return;
        }
        try {
            this.publish('forum/audit/query', {
                type: 'audit_query',
                by: this.currentUser.id,
                timestamp: Date.now()
            }, false);
            this.publish('forum/audit/query', {
                type: 'audit_query',
                by: this.currentUser.id,
                timestamp: Date.now()
            }, false);
            alert('✅ 已向全网广播查询待审核用户的消息。\n\n请等待 3-10 秒后查看结果。\n\n如果对方在线且有注册申请，他们会自动回复。\n\n如果仍看不到，请让对方在登录页点击"📡 重新发送审核请求"按钮。');
            setTimeout(() => {
                this.renderCurrentPage();
            }, 3000);
            setTimeout(() => {
                this.renderCurrentPage();
            }, 6000);
            setTimeout(() => {
                this.renderCurrentPage();
            }, 10000);
        } catch (e) {
            console.error('查询待审核用户失败:', e);
            alert('❌ 查询失败: ' + e.message);
        }
    }

    requestSyncFromOthers() {
        if (!this.connected) {
            alert('⚠️ 当前未连接到网络，请检查网络连接后重试');
            return;
        }
        this.requestSync();
        this.requestSync();
        alert('✅ 已向其他在线用户请求数据同步。\n\n请等待几秒后查看结果。');
        setTimeout(() => {
            this.renderCurrentPage();
        }, 3000);
    }

    approveUser(userId) {
        if (!this.isAdmin(this.currentUser.id)) return;
        this.publish(`forum/audit/${userId}`, {
            type: 'audit_decision',
            userId,
            decision: 'approved',
            by: this.currentUser.id
        }, true);
        this.applyAuditDecision({ userId, decision: 'approved', by: this.currentUser.id });
        alert('已审核通过');
    }

    rejectUser(userId) {
        if (!this.isAdmin(this.currentUser.id)) return;
        this.publish(`forum/audit/${userId}`, {
            type: 'audit_decision',
            userId,
            decision: 'rejected',
            by: this.currentUser.id
        }, true);
        this.applyAuditDecision({ userId, decision: 'rejected', by: this.currentUser.id });
        alert('已拒绝该用户');
    }

    banUser(userId) {
        if (!this.isAdmin(this.currentUser.id)) return;
        this.publish(`forum/ban/${userId}`, {
            type: 'user_ban',
            userId,
            banned: true,
            by: this.currentUser.id
        }, true);
        this.applyUserBan({ userId, banned: true, by: this.currentUser.id });
        alert('已封禁该用户');
    }

    unbanUser(userId) {
        if (!this.isAdmin(this.currentUser.id)) return;
        this.publish(`forum/ban/${userId}`, {
            type: 'user_ban',
            userId,
            banned: false,
            by: this.currentUser.id
        }, true);
        this.applyUserBan({ userId, banned: false, by: this.currentUser.id });
        alert('已解封该用户');
    }

    grantAdmin(userId) {
        if (!this.isAdmin(this.currentUser.id)) return;
        if (this.users[userId] && this.users[userId].role === 'owner') return;
        this.publish(`forum/role/${userId}`, {
            type: 'role_change',
            userId,
            role: 'admin',
            by: this.currentUser.id
        }, true);
        this.applyRoleChange({ userId, role: 'admin', by: this.currentUser.id });
        alert('✅ 已授予管理员权限');
    }

    revokeAdmin(userId) {
        if (!this.isAdmin(this.currentUser.id)) return;
        if (this.users[userId] && this.users[userId].role === 'owner') return;
        this.publish(`forum/role/${userId}`, {
            type: 'role_change',
            userId,
            role: 'user',
            by: this.currentUser.id
        }, true);
        this.applyRoleChange({ userId, role: 'user', by: this.currentUser.id });
        alert('✅ 已撤销管理员权限');
    }

    // ========== 发帖功能 ==========
    openPostModal(isAnnouncement = false) {
        const fullUser = this.currentUser && this.users[this.currentUser.id];
        if (fullUser && fullUser.role === 'pending') {
            alert('⏳ 你的账号正在等待管理员审核，暂时不能发帖。\n审核通过后即可发帖。');
            return;
        }
        document.getElementById('post-modal').style.display = 'flex';
        document.getElementById('post-title').value = '';
        document.getElementById('post-content').value = '';
        document.getElementById('file-list').innerHTML = '';
        const titleEl = document.getElementById('post-title');
        titleEl.placeholder = isAnnouncement ? '📢 公告标题' : '给你的帖子起个标题';
        titleEl.dataset.isAnnouncement = isAnnouncement ? 'true' : 'false';
        document.getElementById('post-modal-title').textContent = isAnnouncement ? '📢 发布公告' : '✏️ 发布新帖';
        document.getElementById('post-files').addEventListener('change', (e) => this.handleFileSelect(e));
    }

    closeModal(id) {
        document.getElementById(id).style.display = 'none';
    }

    handleFileSelect(e) {
        const files = Array.from(e.target.files);
        const list = document.getElementById('file-list');
        const items = files.map(f => {
            const sizeKB = f.size / 1024;
            let sizeText = sizeKB < 1024 ? sizeKB.toFixed(1) + 'KB' : (sizeKB / 1024).toFixed(2) + 'MB';
            let icon = '📎';
            let warn = '';
            if (f.type.startsWith('video/')) {
                    icon = '🎬';
                    if (f.size > 500 * 1024) {
                        warn = ' <span style="color:#ef4444">(超过500KB，将压缩或转为缩略图)</span>';
                    } else {
                        warn = ' <span style="color:#3b82f6">(将发布完整视频)</span>';
                    }
                } else if (f.type.startsWith('image/')) {
                icon = '🖼️';
                if (f.size > 2 * 1024 * 1024) warn = ' <span style="color:#f59e0b">(将自动压缩)</span>';
            }
            return `<div class="file-tag">${icon} ${f.name} (${sizeText})${warn}</div>`;
        });
        list.innerHTML = items.join('');
    }

    async submitPost() {
        const title = document.getElementById('post-title').value.trim();
        const content = document.getElementById('post-content').value.trim();
        const identity = document.getElementById('post-identity').value;
        const filesInput = document.getElementById('post-files');
        const isAnnouncement = document.getElementById('post-title').dataset.isAnnouncement === 'true';

        if (!title || !content) return alert('请填写标题和内容');
        if (!this.currentUser) return alert('请先登录');
        const submitter = this.users[this.currentUser.id];
        if (submitter && submitter.role === 'pending') return alert('⏳ 你的账号正在等待管理员审核，暂时不能发帖。');
        if (isAnnouncement && !this.isAdmin(this.currentUser.id)) return alert('只有管理员可以发布公告');

        const mediaFiles = [];
        const videoLimit = 500 * 1024;
        const postHardLimit = 700 * 1024;
        const postWarnLimit = 600 * 1024;
        let hasLargeVideo = false;
        let hasAnyVideo = false;
        if (filesInput.files.length > 0) {
            for (const file of filesInput.files) {
                if (file.type.startsWith('image/')) {
                    try {
                        const base64 = await this.compressImage(file);
                        mediaFiles.push(base64);
                    } catch (e) {
                        console.error('图片处理失败:', e);
                        alert(`图片 ${file.name} 处理失败`);
                    }
                } else if (file.type.startsWith('video/')) {
                    hasAnyVideo = true;
                    try {
                        if (file.size <= videoLimit) {
                            console.log('🎬 处理视频:', file.name, '大小:', (file.size / 1024).toFixed(0), 'KB');
                            const base64 = await this.fileToBase64(file);
                            mediaFiles.push(base64);
                            console.log('✅ 视频已转为 Base64');
                        } else if (file.size <= 30 * 1024 * 1024) {
                            console.log('🎬 视频较大，尝试压缩:', file.name, '大小:', (file.size / 1024 / 1024).toFixed(2), 'MB');
                            let compressed = null;
                            try {
                                compressed = await this.compressVideo(file, 250);
                            } catch (compressErr) {
                                console.warn('视频压缩失败，准备转缩略图:', compressErr);
                            }
                            if (compressed) {
                                const compressedSize = Math.round(compressed.length / 1024 * 0.75);
                                console.log('✅ 视频压缩完成，约', compressedSize, 'KB');
                                if (compressedSize > 450) {
                                    const thumb = await this.videoToThumbnail(file);
                                    mediaFiles.push(thumb);
                                    hasLargeVideo = true;
                                    console.log('⚠️ 压缩后仍太大，转为缩略图');
                                } else {
                                    mediaFiles.push(compressed);
                                }
                            } else {
                                const thumb = await this.videoToThumbnail(file);
                                mediaFiles.push(thumb);
                                hasLargeVideo = true;
                                console.log('⚠️ 压缩失败，转为缩略图');
                            }
                        } else {
                            const thumb = await this.videoToThumbnail(file);
                            mediaFiles.push(thumb);
                            hasLargeVideo = true;
                        }
                    } catch (e) {
                        console.error('视频处理失败:', e);
                        alert(`视频 ${file.name} 处理失败，建议换个小视频或截图`);
                    }
                }
            }
        }

        const postId = 'post_' + Date.now();
        const postData = {
            id: postId,
            title,
            content,
            authorId: this.currentUser.id,
            isAnonymous: identity === 'anonymous',
            images: mediaFiles,
            isPinned: false,
            isAnnouncement: isAnnouncement,
            timestamp: Date.now(),
            updatedAt: Date.now()
        };

        try {
            const jsonStr = JSON.stringify({ type: 'post', data: postData });
            const sizeKB = jsonStr.length / 1024;
            console.log('📤 帖子大小:', sizeKB.toFixed(1), 'KB');

            let publishConfirmMsg = '';

            if (hasLargeVideo) {
                publishConfirmMsg += '⚠️ 部分视频过大，已转为缩略图发布\n（其他用户只能看到截图，不能播放视频）\n\n提示：视频尽量短、分辨率尽量低。\n\n';
            }

            if (hasAnyVideo && !hasLargeVideo && sizeKB > postWarnLimit) {
                publishConfirmMsg += '⚠️ 帖子包含完整视频，大小 ' + sizeKB.toFixed(1) + 'KB\n\n免费服务器消息大小有限制，接近 700KB 上限可能导致发送失败。\n\n建议：更短更小的视频，或直接发图片。\n\n';
            }

            if (jsonStr.length > postHardLimit) {
                alert('❌ 帖子内容过大 (' + sizeKB.toFixed(1) + 'KB)！\n\n超过 700KB 无法发送。\n\n请用更短更小的视频，或减少图片数量。');
                return;
            }

            if (publishConfirmMsg) {
                if (!confirm(publishConfirmMsg + '是否仍然发布？')) {
                    return;
                }
            }

            this.publish(`forum/posts/${postId}`, { type: 'post', data: postData }, true, (err) => {
                if (err) {
                    console.error('发布失败:', err);
                    alert('❌ 帖子消息发送失败：服务器拒绝了\n\n（可能内容太大。提示：视频尽量短、尽量小，或直接发图片。）');
                } else {
                    this.posts.push(postData);
                    try {
                        this.saveLocalData();
                    } catch (e) {
                        console.error('本地保存失败:', e);
                    }
                    console.log('✅ 帖子已发布');
                }
            });
        } catch (e) {
            console.error('发布异常:', e);
            alert('❌ 发布异常: ' + e.message);
            return;
        }

        this.closeModal('post-modal');
        document.getElementById('post-title').value = '';
        document.getElementById('post-content').value = '';
        document.getElementById('file-list').innerHTML = '';
        filesInput.value = '';
        this.renderCurrentPage();
    }

    async submitComment(postId) {
        const content = document.getElementById('comment-content').value.trim();
        if (!content) return alert('请输入评论内容');
        if (!this.currentUser) return alert('请先登录');
        const commenter = this.users[this.currentUser.id];
        if (commenter && commenter.role === 'pending') return alert('⏳ 你的账号正在等待管理员审核，暂时不能评论。');

        const commentId = 'comment_' + Date.now();
        const commentData = {
            id: commentId,
            postId,
            authorId: this.currentUser.id,
            content,
            timestamp: Date.now(),
            updatedAt: Date.now()
        };

        this.comments.push(commentData);
        this.saveLocalData();
        this.publish(`forum/comments/${commentId}`, { type: 'comment', data: commentData }, true);
        this.router('post', { postId });
    }

    fileToBase64(file) {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = () => resolve(reader.result);
            reader.onerror = reject;
            reader.readAsDataURL(file);
        });
    }

    async compressVideo(file, targetSizeKB = 80) {
        return new Promise((resolve, reject) => {
            const video = document.createElement('video');
            video.muted = true;
            video.playsInline = true;
            video.preload = 'auto';
            const url = URL.createObjectURL(file);
            let resolved = false;

            const cleanup = () => {
                try { URL.revokeObjectURL(url); } catch (e) {}
            };

            video.onerror = () => {
                if (!resolved) { resolved = true; cleanup(); reject(new Error('视频加载失败')); }
            };

            video.onloadedmetadata = async () => {
                try {
                    const duration = video.duration || 1;
                    const maxDim = 480;
                    let width = video.videoWidth || 640;
                    let height = video.videoHeight || 480;
                    if (width > maxDim || height > maxDim) {
                        if (width > height) {
                            height = Math.round(height * maxDim / width);
                            width = maxDim;
                        } else {
                            width = Math.round(width * maxDim / height);
                            height = maxDim;
                        }
                    }

                    const canvas = document.createElement('canvas');
                    canvas.width = width;
                    canvas.height = height;
                    const ctx = canvas.getContext('2d');

                    const tryRecord = async (bitrate) => {
                        return new Promise((res, rej) => {
                            const stream = canvas.captureStream();
                            const audioStream = video.captureStream ? video.captureStream() : null;
                            if (audioStream && audioStream.getAudioTracks().length > 0) {
                                audioStream.getAudioTracks().forEach(track => stream.addTrack(track));
                            }
                            const mimeTypes = ['video/webm;codecs=vp9', 'video/webm;codecs=vp8', 'video/webm'];
                            const mimeType = mimeTypes.find(t => MediaRecorder.isTypeSupported(t)) || 'video/webm';
                            const recorder = new MediaRecorder(stream, { mimeType, videoBitsPerSecond: bitrate });
                            const chunks = [];
                            recorder.ondataavailable = (e) => { if (e.data && e.data.size > 0) chunks.push(e.data); };
                            recorder.onstop = () => {
                                const blob = new Blob(chunks, { type: 'video/webm' });
                                res(blob);
                            };
                            recorder.onerror = (e) => rej(e);

                            video.currentTime = 0;
                            video.play().catch(() => {});
                            recorder.start(100);
                            const drawFrame = () => {
                                if (video.paused || video.ended) return;
                                ctx.fillStyle = '#000000';
                                ctx.fillRect(0, 0, width, height);
                                ctx.drawImage(video, 0, 0, width, height);
                                requestAnimationFrame(drawFrame);
                            };
                            drawFrame();
                            setTimeout(() => {
                                recorder.stop();
                                video.pause();
                            }, Math.min(duration * 1000, 30000));
                        });
                    };

                    let bitrate = 400000;
                    let bestBlob = null;
                    for (let attempt = 0; attempt < 5; attempt++) {
                        const blob = await tryRecord(bitrate);
                        const blobKB = blob.size / 1024;
                        console.log('🎬 视频压缩尝试', attempt + 1, '码率', bitrate, '大小', blobKB.toFixed(1), 'KB');
                        if (!bestBlob || blob.size < bestBlob.size) bestBlob = blob;
                        if (blobKB <= targetSizeKB) break;
                        bitrate = Math.max(50000, Math.round(bitrate * 0.55));
                    }

                    const finalBlob = bestBlob;
                    const reader = new FileReader();
                    reader.onload = () => {
                        resolved = true;
                        cleanup();
                        resolve(reader.result);
                    };
                    reader.onerror = () => {
                        resolved = true;
                        cleanup();
                        reject(new Error('读取压缩后视频失败'));
                    };
                    reader.readAsDataURL(finalBlob);
                } catch (e) {
                    if (!resolved) { resolved = true; cleanup(); reject(e); }
                }
            };

            video.src = url;
        });
    }

    async compressImage(file, maxSize = 400, quality = 0.5) {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = (e) => {
                const img = new Image();
                img.onload = () => {
                    let width = img.width;
                    let height = img.height;
                    if (width > maxSize || height > maxSize) {
                        if (width > height) {
                            height = Math.round(height * maxSize / width);
                            width = maxSize;
                        } else {
                            width = Math.round(width * maxSize / height);
                            height = maxSize;
                        }
                    }
                    const canvas = document.createElement('canvas');
                    canvas.width = width;
                    canvas.height = height;
                    const ctx = canvas.getContext('2d');
                    ctx.fillStyle = '#FFFFFF';
                    ctx.fillRect(0, 0, width, height);
                    ctx.drawImage(img, 0, 0, width, height);
                    let result = canvas.toDataURL('image/jpeg', quality);
                    let currentSize = Math.round(result.length / 1024);
                    console.log('📷 图片压缩:', width + 'x' + height, '初始大小:', currentSize + 'KB');
                    if (currentSize > 100) {
                        result = canvas.toDataURL('image/jpeg', 0.3);
                        currentSize = Math.round(result.length / 1024);
                        console.log('📷 图片二次压缩:', currentSize + 'KB');
                    }
                    if (currentSize > 60) {
                        const smallCanvas = document.createElement('canvas');
                        const scale = width > 300 ? 300 / width : 0.8;
                        smallCanvas.width = Math.round(width * scale);
                        smallCanvas.height = Math.round(height * scale);
                        const smallCtx = smallCanvas.getContext('2d');
                        smallCtx.fillStyle = '#FFFFFF';
                        smallCtx.fillRect(0, 0, smallCanvas.width, smallCanvas.height);
                        smallCtx.drawImage(canvas, 0, 0, smallCanvas.width, smallCanvas.height);
                        result = smallCanvas.toDataURL('image/jpeg', 0.4);
                        currentSize = Math.round(result.length / 1024);
                        console.log('📷 图片三次压缩:', smallCanvas.width + 'x' + smallCanvas.height, currentSize + 'KB');
                    }
                    resolve(result);
                };
                img.onerror = reject;
                img.src = e.target.result;
            };
            reader.onerror = reject;
            reader.readAsDataURL(file);
        });
    }

    async videoToThumbnail(file, seekTime = 1.0) {
        return new Promise((resolve, reject) => {
            const video = document.createElement('video');
            video.preload = 'metadata';
            video.onloadedmetadata = () => {
                video.currentTime = Math.min(seekTime, video.duration / 2);
            };
            video.onseeked = () => {
                const canvas = document.createElement('canvas');
                const maxSize = 320;
                let width = video.videoWidth;
                let height = video.videoHeight;
                if (width > maxSize || height > maxSize) {
                    if (width > height) {
                        height = Math.round(height * maxSize / width);
                        width = maxSize;
                    } else {
                        width = Math.round(width * maxSize / height);
                        height = maxSize;
                    }
                }
                canvas.width = width;
                canvas.height = height;
                const ctx = canvas.getContext('2d');
                ctx.fillStyle = '#000000';
                ctx.fillRect(0, 0, width, height);
                ctx.drawImage(video, 0, 0, width, height);
                const thumbData = canvas.toDataURL('image/jpeg', 0.5);
                video.src = '';
                resolve(thumbData);
            };
            video.onerror = () => {
                reject(new Error('视频无法加载'));
            };
            video.src = URL.createObjectURL(file);
        });
    }

    // ========== 收藏功能 ==========
    toggleFavorite(postId) {
        const idx = this.favorites.indexOf(postId);
        if (idx > -1) {
            this.favorites.splice(idx, 1);
        } else {
            this.favorites.push(postId);
        }
        localStorage.setItem('favorites', JSON.stringify(this.favorites));
        this.renderCurrentPage();
    }

    togglePin(postId) {
        if (!this.currentUser) return;
        const post = this.posts.find(p => p.id === postId);
        if (!post) return;
        if (this.currentUser.id !== post.authorId && !this.isAdmin(this.currentUser.id)) return;
        post.isPinned = !post.isPinned;
        post.updatedAt = Date.now();
        try {
            this.saveLocalData();
        } catch (e) {
            console.warn('本地保存失败:', e);
        }
        try {
            const fullPayload = JSON.stringify({ type: 'post', data: post });
            if (fullPayload.length <= 700 * 1024) {
                this.publish(`forum/posts/${postId}`, { type: 'post', data: post }, true);
            } else {
                const stateOnly = {
                    id: post.id,
                    isPinned: post.isPinned,
                    updatedAt: post.updatedAt,
                    _state_only: true,
                    title: post.title,
                    content: post.content,
                    authorId: post.authorId,
                    isAnonymous: post.isAnonymous,
                    isAnnouncement: post.isAnnouncement,
                    timestamp: post.timestamp
                };
                this.publish(`forum/posts/${postId}`, { type: 'post_state', data: stateOnly }, true);
                console.log('📌 帖子过大，发布状态更新代替完整帖子');
            }
        } catch (e) {
            console.warn('同步置顶状态失败:', e);
        }
        this.renderCurrentPage();
    }

    deletePost(postId) {
        if (!this.currentUser) return;
        const post = this.posts.find(p => p.id === postId);
        if (!post) return;
        if (this.currentUser.id !== post.authorId && !this.isAdmin(this.currentUser.id)) return;
        if (!confirm('确定要删除这条帖子吗？此操作无法撤销。')) return;
        this.posts = this.posts.filter(p => p.id !== postId);
        this.comments = this.comments.filter(c => c.postId !== postId);
        this.favorites = this.favorites.filter(id => id !== postId);
        if (!this.deletedPostIds.includes(postId)) {
            this.deletedPostIds.push(postId);
        }
        try {
            this.saveLocalData();
        } catch (e) {
            console.warn('本地保存失败:', e);
        }
        try {
            const deletedPost = {
                id: postId,
                _deleted: true,
                deletedAt: Date.now(),
                deletedBy: this.currentUser.id
            };
            this.publish(`forum/posts/${postId}`, { type: 'post', data: deletedPost }, true);
            console.log('🗑️ 删除帖子:', postId);
        } catch (e) {
            console.warn('同步删除失败:', e);
        }
        alert('✅ 帖子已删除');
        if (this.currentPage === 'post') {
            this.router('home');
        } else {
            this.renderCurrentPage();
        }
    }

    deleteComment(commentId) {
        if (!this.currentUser) return;
        const comment = this.comments.find(c => c.id === commentId);
        if (!comment) return;
        if (this.currentUser.id !== comment.authorId && !this.isAdmin(this.currentUser.id)) return;
        if (!confirm('确定要删除这条评论吗？此操作无法撤销。')) return;
        this.comments = this.comments.filter(c => c.id !== commentId);
        if (!this.deletedCommentIds.includes(commentId)) {
            this.deletedCommentIds.push(commentId);
        }
        try {
            this.saveLocalData();
        } catch (e) {
            console.warn('本地保存失败:', e);
        }
        try {
            const deletedComment = {
                id: commentId,
                postId: comment.postId,
                _deleted: true,
                deletedAt: Date.now(),
                deletedBy: this.currentUser.id
            };
            this.publish(`forum/comments/${commentId}`, { type: 'comment', data: deletedComment }, true);
            console.log('🗑️ 删除评论:', commentId);
        } catch (e) {
            console.warn('同步删除失败:', e);
        }
        alert('✅ 评论已删除');
        this.renderCurrentPage();
    }

    toggleAnnouncement(postId) {
        if (!this.currentUser || !this.isAdmin(this.currentUser.id)) return;
        const post = this.posts.find(p => p.id === postId);
        if (!post) return;
        post.isAnnouncement = !post.isAnnouncement;
        post.updatedAt = Date.now();
        try {
            this.saveLocalData();
        } catch (e) {
            console.warn('本地保存失败:', e);
        }
        try {
            this.publish(`forum/posts/${postId}`, { type: 'post', data: post }, true);
        } catch (e) {
            console.warn('同步公告状态失败:', e);
        }
        this.renderCurrentPage();
    }

    // ========== 个人主页编辑 ==========
    openProfileEdit() {
        if (!this.currentUser) return;
        const user = this.users[this.currentUser.id] || {};
        document.getElementById('edit-nickname').value = user.nickname || '';
        document.getElementById('edit-bio').value = user.bio || '';
        document.getElementById('edit-bg-color').value = user.bgColor || '#f5f5f5';
        document.getElementById('edit-theme').value = user.theme || 'default';
        const hideProfileEl = document.getElementById('edit-hide-profile');
        if (hideProfileEl) hideProfileEl.checked = !!user.hideProfile;
        document.getElementById('avatar-preview').innerHTML = user.avatar ? `<div class="avatar-preview">${this.renderAvatarContent(user.avatar)}</div>` : '';
        document.getElementById('profile-edit-modal').style.display = 'flex';

        document.getElementById('edit-avatar').addEventListener('change', async (e) => {
            const file = e.target.files[0];
            if (!file) return;
            try {
                const base64 = await this.fileToBase64(file);
                this.tempAvatar = base64;
                document.getElementById('avatar-preview').innerHTML = `<img src="${base64}" class="avatar-preview-img">`;
            } catch (err) {
                alert('头像处理失败');
            }
        });
    }

    saveProfile() {
        if (!this.currentUser) return;
        const hideProfileEl = document.getElementById('edit-hide-profile');
        const updates = {
            nickname: document.getElementById('edit-nickname').value.trim(),
            bio: document.getElementById('edit-bio').value.trim(),
            bgColor: document.getElementById('edit-bg-color').value,
            theme: document.getElementById('edit-theme').value,
            hideProfile: hideProfileEl ? hideProfileEl.checked : false,
            updatedAt: Date.now()
        };
        if (this.tempAvatar) updates.avatar = this.tempAvatar;

        if (!this.users[this.currentUser.id]) {
            this.users[this.currentUser.id] = {
                id: this.currentUser.id,
                username: this.currentUser.username || '',
                nickname: updates.nickname,
                role: 'user',
                banned: false,
                avatar: updates.avatar || '👤',
                bio: updates.bio || '',
                bgColor: updates.bgColor || '#f5f5f5',
                theme: updates.theme || 'default',
                hideProfile: updates.hideProfile
            };
        }
        Object.assign(this.users[this.currentUser.id], updates);
        this.tempAvatar = null;
        this.saveLocalData();

        if (this.currentUser.nickname !== updates.nickname) {
            this.currentUser.nickname = updates.nickname;
            localStorage.setItem('currentUser', JSON.stringify(this.currentUser));
        }

        this.publish(`forum/users/${this.currentUser.id}`, { type: 'user', data: this.users[this.currentUser.id] }, true);
        this.updateUserUI();
        this.closeModal('profile-edit-modal');
        this.router('profile', { userId: this.currentUser.id });
    }

    // ========== 工具方法 ==========
    formatTime(timestamp) {
        if (!timestamp) return '未知时间';
        const now = Date.now();
        const diff = now - timestamp;
        const minute = 60 * 1000;
        const hour = 60 * minute;
        const day = 24 * hour;

        if (diff < minute) return '刚刚';
        if (diff < hour) return `${Math.floor(diff / minute)}分钟前`;
        if (diff < day) return `${Math.floor(diff / hour)}小时前`;
        if (diff < 7 * day) return `${Math.floor(diff / day)}天前`;
        return new Date(timestamp).toLocaleDateString('zh-CN');
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    showImageModal(url) {
        document.getElementById('preview-image').src = url;
        document.getElementById('image-modal').style.display = 'flex';
    }

    sharePost(postId) {
        const url = `${window.location.origin}${window.location.pathname}?post=${postId}`;
        navigator.clipboard.writeText(url).then(() => alert('🔗 链接已复制到剪贴板！'));
    }

    handleSearchInput(e) {
        this.currentSearchQuery = e.target.value;
        const main = document.getElementById('main-content');
        if (!this.currentSearchQuery.trim()) {
            main.innerHTML = this.renderExplore();
            const input = document.getElementById('search-input');
            if (input) {
                input.focus();
                input.setSelectionRange(input.value.length, input.value.length);
            }
            return;
        }
        main.innerHTML = this.renderExplore();
        const input = document.getElementById('search-input');
        if (input) {
            input.focus();
            input.setSelectionRange(input.value.length, input.value.length);
        }
    }

    handleSearch(e) {
        if (e.key !== 'Enter') return;
        this.currentSearchQuery = e.target.value;
        this.renderCurrentPage();
        const input = document.getElementById('search-input');
        if (input) {
            input.focus();
            input.setSelectionRange(input.value.length, input.value.length);
        }
    }

    attachHomeEvents() {}

    // ========== 私聊功能 ==========
    getChatKey(userId1, userId2) {
        return [userId1, userId2].sort().join('|');
    }

    handleChatMessage(msgData) {
        if (!this.currentUser) return;
        if (!msgData || !msgData.from || !msgData.to) {
            console.warn('无效的聊天消息格式:', msgData);
            return;
        }
        if (msgData.from !== this.currentUser.id && msgData.to !== this.currentUser.id) {
            console.log('⚠️ 聊天消息与当前用户无关:', msgData);
            return;
        }
        const key = this.getChatKey(msgData.from, msgData.to);
        if (!this.messages[key]) {
            this.messages[key] = [];
        }
        const exists = this.messages[key].find(m => m.id === msgData.id);
        if (!exists) {
            this.messages[key].push(msgData);
            try {
                this.saveLocalData();
            } catch (e) {
                console.warn('聊天消息本地保存失败:', e);
            }
            const isFromMe = msgData.from === this.currentUser.id;
            const otherId = isFromMe ? msgData.to : msgData.from;
            const otherUser = this.users[otherId] || { nickname: otherId, avatar: '👤' };
            if (!isFromMe) {
                const fr = this.friendRequests.find(r =>
                    ((r.from === otherId && r.to === this.currentUser.id) || (r.from === this.currentUser.id && r.to === otherId)) && r.status !== 'accepted');
                if (fr) {
                    fr.status = 'accepted';
                    if (!fr.acceptedAt) {
                        fr.acceptedAt = Date.now();
                    }
                    try {
                        this.saveLocalData();
                    } catch (e) {}
                    console.log('🤝 收到消息，自动更新好友申请为已同意:', otherUser.nickname);
                }
            }
            console.log('💬 收到新聊天消息:', isFromMe ? '我发送的' : '来自 ' + otherUser.nickname, msgData.content ? msgData.content.substring(0, 50) : '[图片/语音/视频]');
            if (this.currentPage === 'chat') {
                this.refreshChatUI(otherId);
            }
        }
    }

    sendChatMessage(toUserId, content, mediaType = null, mediaData = null, extraData = null) {
        if (!this.currentUser || !toUserId) return;
        const isFriend = this.isFriend(this.currentUser.id, toUserId);
        if (!isFriend) {
            console.warn('⚠️ 不是好友关系，无法发送消息 to:', toUserId);
            return alert('需要对方同意私聊申请后才能发送消息\n\n请先在对方个人主页点击"发送私聊申请"');
        }
        if (!content.trim() && !mediaData) return;
        const msgId = 'msg_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5);
        const msgData = {
            id: msgId,
            from: this.currentUser.id,
            to: toUserId,
            content: content.trim(),
            mediaType: mediaType,
            media: mediaData,
            timestamp: Date.now()
        };
        if (extraData && extraData.duration) {
            msgData.duration = extraData.duration;
        }
        const key = this.getChatKey(this.currentUser.id, toUserId);
        if (!this.messages[key]) {
            this.messages[key] = [];
        }
        this.messages[key].push(msgData);
        try {
            this.saveLocalData();
        } catch (e) {
            console.warn('本地保存聊天失败（可能消息过大）:', e);
        }
        try {
            const chatJsonStr = JSON.stringify({ type: 'chat_message', data: msgData });
            const chatSizeMB = (chatJsonStr.length / 1024 / 1024).toFixed(2);
            console.log('📤 聊天消息大小:', chatSizeMB, 'MB', '类型:', mediaType);
            if (chatJsonStr.length > 500 * 1024 && chatJsonStr.length <= 700 * 1024) {
                if (!confirm('⚠️ 消息较大 (' + chatSizeMB + 'MB)\n\n服务器可能拒绝，对方可能收不到。\n是否仍然发送？')) {
                    return;
                }
            }
            if (chatJsonStr.length > 700 * 1024) {
                alert('❌ 消息超过 700KB，无法发送。\n请用更短更小的视频，或发截图代替');
                return;
            }
        } catch (e) {
            console.warn('计算聊天消息大小失败:', e);
        }
        this.publish(`forum/msg/${toUserId}/${msgId}`, { type: 'chat_message', data: msgData }, true, (err) => {
            if (err) {
                console.error('聊天消息发送失败:', err);
                alert('❌ 消息发送失败：内容太大，对方收不到');
            }
        });
        this.publish(`forum/msg/${this.currentUser.id}/${msgId}`, { type: 'chat_message', data: msgData }, true);
        const toUser = this.users[toUserId] || { nickname: toUserId };
        console.log('📤 发送聊天消息给:', toUser.nickname, '内容:', msgData.content ? msgData.content.substring(0, 50) : '[图片/语音/视频]');
        this.refreshChatUI(toUserId);
    }

    refreshChatUI(friendId) {
        const friend = this.users[friendId] || { id: friendId, nickname: friendId, avatar: '👤' };
        const key = this.getChatKey(this.currentUser.id, friendId);
        const msgs = (this.messages[key] || []).sort((a, b) => a.timestamp - b.timestamp);
        const msgArea = document.getElementById('chat-messages');
        if (!msgArea) return;
        let lastDate = '';
        const messagesHtml = msgs.map(msg => {
            const isMe = msg.from === this.currentUser.id;
            const d = new Date(msg.timestamp);
            const dateStr = d.toLocaleDateString('zh-CN');
            const timeStr = d.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
            let dateDivider = '';
            if (dateStr !== lastDate) {
                dateDivider = `<div class="chat-date-divider"><span>${dateStr}</span></div>`;
                lastDate = dateStr;
            }
            let mediaHtml = '';
            if (msg.mediaType && msg.media) {
                if (msg.mediaType === 'image') {
                    mediaHtml = `<div style="margin-top:8px;"><img src="${msg.media}" style="max-width:200px;max-height:200px;border-radius:12px;display:block;cursor:zoom-in;" onclick="app.showImageModal('${msg.media}')"></div>`;
                } else if (msg.mediaType === 'video') {
                    mediaHtml = `<div style="margin-top:8px;"><video src="${msg.media}" controls muted preload="metadata" style="max-width:260px;max-height:240px;border-radius:12px;display:block;"></video></div>`;
                } else if (msg.mediaType === 'voice') {
                    const dur = msg.duration ? Math.round(msg.duration) + ' 秒' : '语音';
                    mediaHtml = `<div style="margin-top:8px;display:flex;align-items:center;gap:8px;min-width:160px;">
                        <button onclick="this.nextElementSibling.play()" style="background:transparent;border:none;font-size:1.3rem;cursor:pointer;">▶️</button>
                        <audio src="${msg.media}" style="max-width:200px;"></audio>
                        <span style="font-size:0.85rem;opacity:0.7;">🎙️ ${dur}</span>
                    </div>`;
                }
            }
            const textHtml = msg.content ? `<div class="msg-content">${this.escapeHtml(msg.content)}</div>` : '';
            return `
                ${dateDivider}
                <div class="chat-message ${isMe ? 'mine' : 'theirs'}">
                    ${!isMe ? `<div class="msg-avatar">${this.renderAvatarContent(friend.avatar)}</div>` : ''}
                    <div class="msg-bubble">
                        ${textHtml}
                        ${mediaHtml}
                        <div class="msg-time">${timeStr}</div>
                    </div>
                    ${isMe ? `<div class="msg-avatar">${this.renderAvatarContent(this.users[this.currentUser.id] ? this.users[this.currentUser.id].avatar : '👤')}</div>` : ''}
                </div>
            `;
        }).join('');
        msgArea.innerHTML = msgs.length === 0 ? `
            <div class="empty-state" style="margin-top:100px;">
                <div class="empty-icon">👋</div>
                <p>还没有消息，发送第一条消息开始聊天吧！</p>
            </div>
        ` : messagesHtml;
        setTimeout(() => {
            msgArea.scrollTop = msgArea.scrollHeight;
        }, 50);
    }

    async submitChatFile(friendId) {
        const input = document.createElement('input');
        input.type = 'file';
        input.accept = 'image/*,video/*';
        input.onchange = async (e) => {
            const files = Array.from(e.target.files);
            for (const file of files) {
                try {
                    if (file.type.startsWith('image/')) {
                        const base64 = await this.compressImage(file);
                        this.sendChatMessage(friendId, '', 'image', base64);
                    } else if (file.type.startsWith('video/')) {
                        if (file.size <= 1 * 1024 * 1024) {
                            console.log('💬 处理聊天视频:', file.name, '大小:', (file.size / 1024 / 1024).toFixed(2), 'MB');
                            const base64 = await this.fileToBase64(file);
                            this.sendChatMessage(friendId, '', 'video', base64);
                        } else {
                            const thumb = await this.videoToThumbnail(file);
                            this.sendChatMessage(friendId, '', 'video', thumb);
                            alert('视频超过 1MB，已转为缩略图发送\n（完整视频太大对方收不到，请用更短的视频或截图）');
                        }
                    }
                } catch (err) {
                    console.error('文件处理失败:', err);
                    alert('文件处理失败');
                }
            }
        };
        input.click();
    }

    startVoiceRecord(friendId) {
        if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
            alert('你的浏览器不支持语音录制');
            return;
        }
        if (this._voiceRecorder && this._voiceRecorder.state !== 'inactive') {
            this._voiceRecorder.stop();
            return;
        }
        navigator.mediaDevices.getUserMedia({ audio: true }).then(stream => {
            const recorder = new MediaRecorder(stream);
            const chunks = [];
            const startTime = Date.now();
            this._voiceStream = stream;
            this._voiceRecorder = recorder;
            const btn = document.getElementById('voice-btn');
            if (btn) {
                btn.innerHTML = '⏺️';
                btn.style.background = '#ef4444';
                btn.style.color = 'white';
                btn.title = '点击停止';
            }
            recorder.ondataavailable = (e) => { if (e.data.size > 0) chunks.push(e.data); };
            recorder.onstop = () => {
                const duration = (Date.now() - startTime) / 1000;
                stream.getTracks().forEach(t => t.stop());
                if (duration < 1) {
                    alert('录音太短了');
                    if (btn) { btn.innerHTML = '🎙️'; btn.style.background = ''; btn.style.color = ''; btn.title = '按住/点击开始录音'; }
                    return;
                }
                const blob = new Blob(chunks, { type: chunks[0] && chunks[0].type ? chunks[0].type : 'audio/webm' });
                const reader = new FileReader();
                reader.onloadend = () => {
                    this.sendChatMessage(friendId, '', 'voice', reader.result, { duration: duration });
                };
                reader.readAsDataURL(blob);
                if (btn) { btn.innerHTML = '🎙️'; btn.style.background = ''; btn.style.color = ''; btn.title = '点击开始录音'; }
                this._voiceRecorder = null;
                this._voiceStream = null;
            };
            recorder.start();
        }).catch(err => {
            console.error('获取麦克风失败:', err);
            alert('无法获取麦克风权限，请检查浏览器设置');
        });
    }

    submitChatMessage(friendId) {
        const input = document.getElementById('chat-input');
        if (!input) return;
        const content = input.value;
        if (!content.trim()) return;
        this.sendChatMessage(friendId, content);
        input.value = '';
        input.focus();
    }

    getChatContacts() {
        const contacts = new Set();
        Object.keys(this.messages).forEach(key => {
            const parts = key.split('|');
            if (parts.includes(this.currentUser.id)) {
                parts.forEach(p => {
                    if (p !== this.currentUser.id) contacts.add(p);
                });
            }
        });
        this.friendRequests.forEach(r => {
            if (r.status === 'accepted') {
                if (r.from === this.currentUser.id) contacts.add(r.to);
                if (r.to === this.currentUser.id) contacts.add(r.from);
            }
        });
        return Array.from(contacts);
    }

    getLastMessage(userId) {
        const key = this.getChatKey(this.currentUser.id, userId);
        const msgs = this.messages[key] || [];
        return msgs.length > 0 ? msgs[msgs.length - 1] : null;
    }

    isFriend(userId1, userId2) {
        if (!userId1 || !userId2) return false;
        const hasAcceptedRequest = this.friendRequests.some(r =>
            ((r.from === userId1 && r.to === userId2) || (r.from === userId2 && r.to === userId1)) &&
            r.status === 'accepted'
        );
        if (hasAcceptedRequest) return true;
        const key = this.getChatKey(userId1, userId2);
        const hasMessages = this.messages[key] && this.messages[key].length > 0;
        if (hasMessages) {
            console.log('💡 isFriend: 通过聊天消息记录判断为好友关系', userId1, '↔', userId2);
            return true;
        }
        return false;
    }

    getFriendRequestStatus(userId) {
        if (!this.currentUser || !userId) return null;
        const me = this.currentUser.id;
        const request = this.friendRequests.find(r =>
            ((r.from === me && r.to === userId) || (r.from === userId && r.to === me))
        );
        if (!request) return null;
        if (request.status === 'accepted') return 'accepted';
        if (request.status === 'rejected') return 'rejected';
        if (request.from === me) return 'waiting';
        return 'received';
    }

    sendFriendRequest(toUserId) {
        if (!this.currentUser) return;
        const me = this.currentUser.id;
        const existing = this.friendRequests.find(r =>
            ((r.from === me && r.to === toUserId) || (r.from === toUserId && r.to === me))
        );
        if (existing) {
            if (existing.status === 'accepted') return alert('你们已经是好友了');
            if (existing.status === 'pending') {
                if (existing.from === me) return alert('你已经发送过申请，请等待对方回应');
                return;
            }
        }
        const req = {
            id: 'fr_' + Date.now(),
            from: me,
            to: toUserId,
            status: 'pending',
            timestamp: Date.now()
        };
        this.friendRequests.push(req);
        this.saveLocalData();
        try {
            this.publish(`forum/friends/${toUserId}`, { type: 'friendRequest', data: req }, true);
            this.publish(`forum/friends/${me}`, { type: 'friendRequest', data: req }, true);
            console.log('📤 发送私聊申请:', req);
        } catch (e) { console.warn('同步好友申请失败:', e); }
        alert('✅ 私聊申请已发送！等待对方同意后即可开始聊天\n\n提示：如果对方在线，对方会立即收到通知。如果对方稍后打开页面，也会收到这个申请。');
        this.renderCurrentPage();
    }

    acceptFriendRequest(fromUserId) {
        if (!this.currentUser) return;
        const me = this.currentUser.id;
        const req = this.friendRequests.find(r => r.from === fromUserId && r.to === me && r.status === 'pending');
        if (!req) return;
        req.status = 'accepted';
        req.acceptedAt = Date.now();
        this.saveLocalData();
        try {
            this.publish(`forum/friends/${fromUserId}`, { type: 'friendRequest', data: req }, true);
            this.publish(`forum/friends/${me}`, { type: 'friendRequest', data: req }, true);
            console.log('✅ 同意私聊申请:', req);
        } catch (e) { console.warn('同步好友状态失败:', e); }
        alert('✅ 已同意私聊申请！现在可以聊天了');
        this.renderCurrentPage();
    }

    rejectFriendRequest(fromUserId) {
        if (!this.currentUser) return;
        const me = this.currentUser.id;
        const req = this.friendRequests.find(r => r.from === fromUserId && r.to === me && r.status === 'pending');
        if (!req) return;
        req.status = 'rejected';
        this.saveLocalData();
        try {
            this.publish(`forum/friends/${fromUserId}`, { type: 'friendRequest', data: req }, true);
            this.publish(`forum/friends/${me}`, { type: 'friendRequest', data: req }, true);
            console.log('❌ 拒绝私聊申请:', req);
        } catch (e) { console.warn('同步好友状态失败:', e); }
        alert('已拒绝私聊申请');
        this.renderCurrentPage();
    }

    mergeFriendRequest(req) {
        if (!req || !req.id) return;
        const existingIdx = this.friendRequests.findIndex(r => r.id === req.id);
        let changed = false;
        let statusChanged = false;
        let oldStatus = null;
        if (existingIdx === -1) {
            this.friendRequests.push(req);
            changed = true;
            statusChanged = true;
            const otherId = req.from === this.currentUser?.id ? req.to : req.from;
            const otherUser = this.users[otherId] || { nickname: otherId };
            console.log('📨 收到新的私聊申请:', req.status, '来自/发给:', otherUser.nickname);
        } else {
            const existing = this.friendRequests[existingIdx];
            oldStatus = existing.status;
            const newTs = req.acceptedAt || req.timestamp || 0;
            const oldTs = existing.acceptedAt || existing.timestamp || 0;
            if (newTs >= oldTs) {
                this.friendRequests[existingIdx] = req;
                changed = true;
                if (oldStatus !== req.status) {
                    statusChanged = true;
                    const otherId = req.from === this.currentUser?.id ? req.to : req.from;
                    const otherUser = this.users[otherId] || { nickname: otherId };
                    console.log('🔄 私聊申请状态变化:', oldStatus, '→', req.status, '涉及用户:', otherUser.nickname);
                }
            }
        }
        if (changed) {
            this.saveLocalData();
            if (statusChanged && req.status === 'accepted' && oldStatus !== 'accepted') {
                const otherId = req.from === this.currentUser?.id ? req.to : req.from;
                const otherUser = this.users[otherId] || { nickname: otherId };
                console.log('✅ 好友关系已建立，可以开始聊天了:', otherUser.nickname);
            }
        }
    }

    renderChatList() {
        const contacts = this.getChatContacts();
        const receivedRequests = this.friendRequests.filter(r => r.to === this.currentUser.id && r.status === 'pending');
        let requestHtml = '';
        if (receivedRequests.length > 0) {
            requestHtml = `
                <div class="friend-requests-section">
                    <h3>📨 私聊申请 (${receivedRequests.length})</h3>
                    ${receivedRequests.map(r => {
                        const fromUser = this.users[r.from] || { id: r.from, nickname: r.from, avatar: '👤' };
                        return `
                            <div class="friend-request-item">
                                <div class="chat-avatar">${this.renderAvatarContent(fromUser.avatar)}</div>
                                <div class="chat-info">
                                    <div class="chat-name-row">
                                        <span class="chat-name">${fromUser.nickname || fromUser.id}</span>
                                    </div>
                                    <div class="chat-preview" style="color:#f59e0b;">想和你聊天</div>
                                </div>
                                <div class="request-actions">
                                    <button onclick="app.acceptFriendRequest('${r.from}')" class="btn btn-primary" style="padding:6px 12px;font-size:0.85rem;">✅ 同意</button>
                                    <button onclick="app.rejectFriendRequest('${r.from}')" class="btn btn-secondary" style="padding:6px 12px;font-size:0.85rem;">❌ 拒绝</button>
                                </div>
                            </div>
                        `;
                    }).join('')}
                </div>
            `;
        }
        const myGroups = this.getGroupContacts();
        const groupItems = myGroups.map(group => {
            const lastMsg = (this.groupMessages[group.id] || []).slice(-1)[0];
            let preview = '点击开始群聊';
            if (lastMsg) {
                const sender = this.users[lastMsg.from] || { nickname: '未知用户' };
                if (lastMsg.content) {
                    preview = `${sender.nickname}: ${lastMsg.content.slice(0, 40)}`;
                } else if (lastMsg.mediaType === 'image') {
                    preview = `${sender.nickname}: [图片]`;
                } else if (lastMsg.mediaType === 'video') {
                    preview = `${sender.nickname}: [视频]`;
                } else if (lastMsg.mediaType === 'voice') {
                    preview = `${sender.nickname}: [语音]`;
                }
            }
            const time = lastMsg ? new Date(lastMsg.timestamp).toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' }) : '';
            return `
                <div class="chat-item" onclick="app.router('group_chat', { groupId: '${group.id}' })" style="background:#f0f9ff;border-left:3px solid #3b82f6;">
                    <div class="chat-avatar" style="font-size:1.3rem;">${group.avatar}</div>
                    <div class="chat-info">
                        <div class="chat-name-row">
                            <span class="chat-name">${group.name}</span>
                            <span class="chat-time">${time}</span>
                        </div>
                        <div class="chat-preview">${preview}</div>
                    </div>
                </div>
            `;
        }).join('');

        if (contacts.length === 0 && myGroups.length === 0) {
            return `
                <div class="chats-page">
                    <div class="page-header" style="display:flex;justify-content:space-between;align-items:center;">
                        <h2>💬 消息</h2>
                        <button onclick="app.router('group_create')" class="btn btn-primary" style="padding:6px 14px;font-size:0.9rem;">👥 建群</button>
                    </div>
                    ${requestHtml}
                    <div class="empty-state">
                        <div class="empty-icon">📭</div>
                        <h3>还没有聊天</h3>
                        <p>去帖子页面看看其他人的主页，点击"发送私聊申请"开始聊天吧</p>
                    </div>
                </div>
            `;
        }
        const contactsWithTime = contacts.map(userId => {
            const user = this.users[userId] || { id: userId, nickname: userId, avatar: '👤' };
            const lastMsg = this.getLastMessage(userId);
            const lastTime = lastMsg ? lastMsg.timestamp : 0;
            return { userId, user, lastMsg, lastTime };
        });
        contactsWithTime.sort((a, b) => b.lastTime - a.lastTime);
        const chatItems = contactsWithTime.map(({ userId, user, lastMsg, lastTime }) => {
            let preview = '点击开始聊天';
            if (lastMsg) {
                if (lastMsg.content) {
                    preview = lastMsg.content.slice(0, 50);
                } else if (lastMsg.mediaType === 'image') {
                    preview = '[图片]';
                } else if (lastMsg.mediaType === 'video') {
                    preview = '[视频]';
                } else if (lastMsg.mediaType === 'voice') {
                    preview = '[语音]';
                }
            }
            const time = lastMsg ? new Date(lastMsg.timestamp).toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' }) : '';
            return `
                <div class="chat-item" onclick="app.router('chat', { friendId: '${userId}' })">
                    <div class="chat-avatar">${this.renderAvatarContent(user.avatar)}</div>
                    <div class="chat-info">
                        <div class="chat-name-row">
                            <span class="chat-name">${user.nickname || userId}</span>
                            <span class="chat-time">${time}</span>
                        </div>
                        <div class="chat-preview">${preview}</div>
                    </div>
                </div>
            `;
        }).join('');
        return `
            <div class="chats-page">
                <div class="page-header" style="display:flex;justify-content:space-between;align-items:center;">
                    <h2>💬 消息</h2>
                    <button onclick="app.router('group_create')" class="btn btn-primary" style="padding:6px 14px;font-size:0.9rem;">👥 建群</button>
                </div>
                ${requestHtml}
                ${myGroups.length > 0 ? `<div class="chat-list">${groupItems}</div>` : ''}
                ${contacts.length > 0 ? `<div class="chat-list">${chatItems}</div>` : ''}
            </div>
        `;
    }

    renderChat(friendId) {
        if (!friendId) {
            return this.renderChatList();
        }
        const chatKey = this.getChatKey(this.currentUser.id, friendId);
        const hasChatMessages = this.messages[chatKey] && this.messages[chatKey].length > 0;
        const hasAcceptedRequest = this.friendRequests.some(r =>
            ((r.from === this.currentUser.id && r.to === friendId) || (r.from === friendId && r.to === this.currentUser.id)) &&
            r.status === 'accepted');
        if (!hasAcceptedRequest && !hasChatMessages) {
            const status = this.getFriendRequestStatus(friendId);
            const friend = this.users[friendId] || { id: friendId, nickname: friendId, avatar: '👤' };
            let statusHtml = '';
            if (status === 'waiting') {
                statusHtml = `
                    <div class="empty-state" style="margin-top:100px;">
                        <div class="empty-icon">⏳</div>
                        <h3>等待对方同意私聊申请</h3>
                        <p>你发送的私聊申请正在等待 ${friend.nickname} 同意</p>
                    </div>
                `;
            } else if (status === 'received') {
                statusHtml = `
                    <div class="empty-state" style="margin-top:100px;">
                        <div class="empty-icon">📨</div>
                        <h3>${friend.nickname} 想和你聊天</h3>
                        <p style="margin-bottom:20px;">同意后才能开始互相发送消息</p>
                        <div style="display:flex;gap:12px;justify-content:center;">
                            <button onclick="app.acceptFriendRequest('${friendId}')" class="btn btn-primary">✅ 同意</button>
                            <button onclick="app.rejectFriendRequest('${friendId}')" class="btn btn-secondary">❌ 拒绝</button>
                        </div>
                    </div>
                `;
            } else if (status === 'rejected') {
                statusHtml = `
                    <div class="empty-state" style="margin-top:100px;">
                        <div class="empty-icon">❌</div>
                        <h3>对方已拒绝你的私聊申请</h3>
                        <p>你可以重新发送申请</p>
                        <button onclick="app.sendFriendRequest('${friendId}')" class="btn btn-primary" style="margin-top:16px;">📨 重新发送申请</button>
                    </div>
                `;
            } else {
                statusHtml = `
                    <div class="empty-state" style="margin-top:100px;">
                        <div class="empty-icon">🔒</div>
                        <h3>需要先发送私聊申请</h3>
                        <p>为了保护用户隐私，双方需同意后才能聊天</p>
                        <button onclick="app.sendFriendRequest('${friendId}')" class="btn btn-primary" style="margin-top:16px;">📨 发送私聊申请</button>
                    </div>
                `;
            }
            return `
                <div class="chat-page">
                    <div class="chat-header">
                        <button class="back-btn" onclick="app.router('chats')">←</button>
                        <div class="chat-header-avatar">${this.renderAvatarContent(friend.avatar)}</div>
                        <div class="chat-header-info">
                            <h3>${friend.nickname || friendId}</h3>
                        </div>
                    </div>
                    <div class="chat-messages">
                        ${statusHtml}
                    </div>
                </div>
            `;
        }
        this.currentFriendId = friendId;
        const friend = this.users[friendId] || { id: friendId, nickname: friendId, avatar: '👤' };
        const key = this.getChatKey(this.currentUser.id, friendId);
        const msgs = (this.messages[key] || []).sort((a, b) => a.timestamp - b.timestamp);
        let lastDate = '';
        const messagesHtml = msgs.map(msg => {
            const isMe = msg.from === this.currentUser.id;
            const d = new Date(msg.timestamp);
            const dateStr = d.toLocaleDateString('zh-CN');
            const timeStr = d.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
            let dateDivider = '';
            if (dateStr !== lastDate) {
                dateDivider = `<div class="chat-date-divider"><span>${dateStr}</span></div>`;
                lastDate = dateStr;
            }
            let mediaHtml = '';
            if (msg.mediaType && msg.media) {
                if (msg.mediaType === 'image') {
                    mediaHtml = `<div style="margin-top:8px;"><img src="${msg.media}" style="max-width:200px;max-height:200px;border-radius:12px;display:block;cursor:zoom-in;" onclick="app.showImageModal('${msg.media}')"></div>`;
                } else if (msg.mediaType === 'video') {
                    mediaHtml = `<div style="margin-top:8px;"><video src="${msg.media}" controls muted preload="metadata" style="max-width:260px;max-height:240px;border-radius:12px;display:block;"></video></div>`;
                } else if (msg.mediaType === 'voice') {
                    const dur = msg.duration ? Math.round(msg.duration) + ' 秒' : '语音';
                    mediaHtml = `<div style="margin-top:8px;display:flex;align-items:center;gap:8px;min-width:160px;">
                        <button onclick="this.nextElementSibling.play()" style="background:transparent;border:none;font-size:1.3rem;cursor:pointer;">▶️</button>
                        <audio src="${msg.media}" style="max-width:200px;"></audio>
                        <span style="font-size:0.85rem;opacity:0.7;">🎙️ ${dur}</span>
                    </div>`;
                }
            }
            const textHtml = msg.content ? `<div class="msg-content">${this.escapeHtml(msg.content)}</div>` : '';
            return `
                ${dateDivider}
                <div class="chat-message ${isMe ? 'mine' : 'theirs'}">
                    ${!isMe ? `<div class="msg-avatar">${this.renderAvatarContent(friend.avatar)}</div>` : ''}
                    <div class="msg-bubble">
                        ${textHtml}
                        ${mediaHtml}
                        <div class="msg-time">${timeStr}</div>
                    </div>
                    ${isMe ? `<div class="msg-avatar">${this.renderAvatarContent(this.users[this.currentUser.id] ? this.users[this.currentUser.id].avatar : '👤')}</div>` : ''}
                </div>
            `;
        }).join('');
        return `
            <div class="chat-page">
                <div class="chat-header">
                    <button class="back-btn" onclick="app.router('chats')">←</button>
                    <div class="chat-header-avatar">${this.renderAvatarContent(friend.avatar)}</div>
                    <div class="chat-header-info">
                        <h3>${friend.nickname || friendId}</h3>
                    </div>
                </div>
                <div class="chat-messages" id="chat-messages">
                    ${msgs.length === 0 ? `
                        <div class="empty-state" style="margin-top:100px;">
                            <div class="empty-icon">👋</div>
                            <p>还没有消息，发送第一条消息开始聊天吧！</p>
                        </div>
                    ` : messagesHtml}
                </div>
                <div class="chat-input-bar">
                    <button class="chat-media-btn" onclick="app.submitChatFile('${friendId}')" title="发送图片/视频">📎</button>
                    <button class="chat-media-btn" id="voice-btn" onclick="app.startVoiceRecord('${friendId}')" title="点击开始录音">🎙️</button>
                    <input type="text" id="chat-input" placeholder="输入消息..." onkeydown="if(event.key==='Enter'){event.preventDefault();app.submitChatMessage('${friendId}');}" autocomplete="off">
                    <button class="btn btn-primary" onclick="app.submitChatMessage('${friendId}')">发送</button>
                </div>
            </div>
        `;
    }

    // ========== 小群聊功能 ==========
    createGroup(name, memberIds) {
        if (!this.currentUser) return alert('请先登录');
        if (!name.trim()) return alert('请输入群名称');
        const allMembers = [...new Set([this.currentUser.id, ...memberIds])];
        if (allMembers.length < 2) return alert('至少邀请一位好友');
        const groupId = 'group_' + Date.now();
        const group = {
            id: groupId,
            name: name.trim(),
            members: allMembers,
            createdBy: this.currentUser.id,
            timestamp: Date.now(),
            avatar: '👥',
            removedMembers: []
        };
        this.groups.push(group);
        this.saveLocalData();
        this.publish(`forum/groupmeta/${groupId}`, { type: 'group_meta', data: group }, true);
        alert('✅ 群聊创建成功！');
        this.router('group_chat', { groupId });
    }

    mergeGroupMeta(groupData) {
        if (!groupData || !groupData.id) return;
        const existingIdx = this.groups.findIndex(g => g.id === groupData.id);
        if (existingIdx >= 0) {
            const existing = this.groups[existingIdx];
            const newTs = groupData.timestamp || 0;
            const oldTs = existing.timestamp || 0;
            if (newTs >= oldTs) {
                this.groups[existingIdx] = groupData;
                this.saveLocalData();
                console.log('🔄 更新群组信息:', groupData.name);
            }
        } else {
            this.groups.push(groupData);
            this.saveLocalData();
            console.log('📥 收到新群组:', groupData.name);
        }
        if (this.currentPage === 'chats') {
            this.renderCurrentPage();
        }
    }

    sendGroupMessage(groupId, content, mediaType = null, mediaData = null, extraData = null) {
        if (!this.currentUser) return;
        const group = this.groups.find(g => g.id === groupId);
        if (!group) return alert('群聊不存在');
        const isMember = group.members.includes(this.currentUser.id) && !group.removedMembers?.includes(this.currentUser.id);
        if (!isMember) return alert('你已不在该群聊中');
        const msgId = 'gm_' + Date.now() + '_' + Math.random().toString(36).substr(2, 5);
        const msgData = {
            id: msgId,
            from: this.currentUser.id,
            groupId: groupId,
            content: content || '',
            mediaType: mediaType,
            media: mediaData,
            timestamp: Date.now(),
            ...extraData
        };
        if (!this.groupMessages[groupId]) {
            this.groupMessages[groupId] = [];
        }
        this.groupMessages[groupId].push(msgData);
        try {
            this.saveLocalData();
        } catch (e) {
            console.warn('群聊消息本地保存失败:', e);
        }
        try {
            const chatJsonStr = JSON.stringify({ type: 'group_message', data: msgData });
            const chatSizeMB = (chatJsonStr.length / 1024 / 1024).toFixed(2);
            console.log('📤 群聊消息大小:', chatSizeMB, 'MB', '类型:', mediaType);
            if (chatJsonStr.length > 900 * 1024) {
                alert('❌ 消息超过 900KB，无法发送。\n请用更短更小的视频，或发截图代替');
                return;
            }
        } catch (e) {
            console.warn('计算群聊消息大小失败:', e);
        }
        this.publish(`forum/group/${groupId}/${msgId}`, { type: 'group_message', data: msgData }, true, (err) => {
            if (err) {
                console.error('群聊消息发送失败:', err);
                alert('❌ 消息发送失败：内容太大，群成员收不到');
            }
        });
        this.refreshGroupChatUI(groupId);
    }

    handleGroupMessage(msgData) {
        if (!this.currentUser) return;
        if (!msgData || !msgData.groupId) {
            console.warn('无效的群聊消息格式:', msgData);
            return;
        }
        const group = this.groups.find(g => g.id === msgData.groupId);
        if (!group) {
            console.log('⚠️ 收到群聊消息但群组不存在:', msgData.groupId);
            return;
        }
        const isMember = group.members.includes(this.currentUser.id) && !group.removedMembers?.includes(this.currentUser.id);
        if (!isMember) {
            console.log('⚠️ 群聊消息与当前用户无关:', msgData.groupId);
            return;
        }
        if (!this.groupMessages[msgData.groupId]) {
            this.groupMessages[msgData.groupId] = [];
        }
        const exists = this.groupMessages[msgData.groupId].find(m => m.id === msgData.id);
        if (!exists) {
            this.groupMessages[msgData.groupId].push(msgData);
            try {
                this.saveLocalData();
            } catch (e) {
                console.warn('群聊消息本地保存失败:', e);
            }
            const fromUser = this.users[msgData.from] || { nickname: msgData.from, avatar: '👤' };
            console.log('💬 收到群聊消息:', '来自 ' + fromUser.nickname, msgData.content ? msgData.content.substring(0, 50) : '[图片/语音/视频]');
            if (this.currentPage === 'group_chat' && this.currentGroupId === msgData.groupId) {
                this.refreshGroupChatUI(msgData.groupId);
            }
        }
    }

    leaveGroup(groupId) {
        if (!this.currentUser) return;
        const group = this.groups.find(g => g.id === groupId);
        if (!group) return;
        if (!confirm(`确定退出群聊 "${group.name}" 吗？`)) return;
        group.members = group.members.filter(id => id !== this.currentUser.id);
        if (!group.removedMembers) group.removedMembers = [];
        group.removedMembers.push(this.currentUser.id);
        group.timestamp = Date.now();
        this.saveLocalData();
        this.publish(`forum/groupmeta/${groupId}`, { type: 'group_meta', data: group }, true);
        alert('已退出群聊');
        this.router('chats');
    }

    refreshGroupChatUI(groupId) {
        const group = this.groups.find(g => g.id === groupId);
        if (!group) return;
        const msgs = (this.groupMessages[groupId] || []).sort((a, b) => a.timestamp - b.timestamp);
        const msgArea = document.getElementById('group-chat-messages');
        if (!msgArea) return;
        let lastDate = '';
        const messagesHtml = msgs.map(msg => {
            const isMe = msg.from === this.currentUser.id;
            const fromUser = this.users[msg.from] || { nickname: '未知用户', avatar: '👤' };
            const d = new Date(msg.timestamp);
            const dateStr = d.toLocaleDateString('zh-CN');
            const timeStr = d.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
            let dateDivider = '';
            if (dateStr !== lastDate) {
                dateDivider = `<div class="chat-date-divider"><span>${dateStr}</span></div>`;
                lastDate = dateStr;
            }
            let mediaHtml = '';
            if (msg.mediaType && msg.media) {
                if (msg.mediaType === 'image') {
                    mediaHtml = `<div style="margin-top:8px;"><img src="${msg.media}" style="max-width:200px;max-height:200px;border-radius:12px;display:block;cursor:zoom-in;" onclick="app.showImageModal('${msg.media}')"></div>`;
                } else if (msg.mediaType === 'video') {
                    mediaHtml = `<div style="margin-top:8px;"><video src="${msg.media}" controls muted preload="metadata" style="max-width:260px;max-height:240px;border-radius:12px;display:block;"></video></div>`;
                } else if (msg.mediaType === 'voice') {
                    const dur = msg.duration ? Math.round(msg.duration) + ' 秒' : '语音';
                    mediaHtml = `<div style="margin-top:8px;display:flex;align-items:center;gap:8px;min-width:160px;">
                        <button onclick="this.nextElementSibling.play()" style="background:transparent;border:none;font-size:1.3rem;cursor:pointer;">▶️</button>
                        <audio src="${msg.media}" style="max-width:200px;"></audio>
                        <span style="font-size:0.85rem;opacity:0.7;">🎙️ ${dur}</span>
                    </div>`;
                }
            }
            const textHtml = msg.content ? `<div class="msg-content">${this.escapeHtml(msg.content)}</div>` : '';
            return `
                ${dateDivider}
                <div class="chat-message ${isMe ? 'mine' : 'theirs'}">
                    ${!isMe ? `<div class="msg-avatar">${this.renderAvatarContent(fromUser.avatar)}</div>` : ''}
                    <div class="msg-bubble">
                        ${!isMe ? `<div style="font-size:0.75rem;opacity:0.6;margin-bottom:2px;">${fromUser.nickname}</div>` : ''}
                        ${textHtml}
                        ${mediaHtml}
                        <div class="msg-time">${timeStr}</div>
                    </div>
                </div>
            `;
        }).join('');
        msgArea.innerHTML = messagesHtml;
        msgArea.scrollTop = msgArea.scrollHeight;
    }

    submitGroupChatMessage(groupId) {
        const input = document.getElementById('group-chat-input');
        if (!input) return;
        const content = input.value;
        if (!content.trim()) return;
        this.sendGroupMessage(groupId, content);
        input.value = '';
        input.focus();
    }

    async submitGroupChatFile(groupId) {
        const input = document.createElement('input');
        input.type = 'file';
        input.accept = 'image/*,video/*';
        input.onchange = async () => {
            const file = input.files[0];
            if (!file) return;
            try {
                if (file.type.startsWith('image/')) {
                    const base64 = await this.compressImage(file);
                    this.sendGroupMessage(groupId, '', 'image', base64);
                } else if (file.type.startsWith('video/')) {
                    if (file.size <= 1 * 1024 * 1024) {
                        const base64 = await this.fileToBase64(file);
                        this.sendGroupMessage(groupId, '', 'video', base64);
                    } else {
                        const thumb = await this.videoToThumbnail(file);
                        this.sendGroupMessage(groupId, '', 'video', thumb);
                        alert('视频超过 1MB，已转为缩略图发送');
                    }
                }
            } catch (err) {
                console.error('文件处理失败:', err);
                alert('文件处理失败');
            }
        };
        input.click();
    }

    getGroupContacts() {
        if (!this.currentUser) return [];
        return this.groups.filter(g =>
            g.members.includes(this.currentUser.id) &&
            !g.removedMembers?.includes(this.currentUser.id)
        );
    }

    renderGroupCreate() {
        const friends = this.getChatContacts().map(id => this.users[id] || { id, nickname: id, avatar: '👤' });
        return `
            <div class="auth-page">
                <div class="auth-container" style="max-width:480px;">
                    <div style="display:flex;align-items:center;gap:12px;margin-bottom:20px;">
                        <button onclick="app.router('chats')" class="btn btn-secondary" style="padding:6px 12px;">← 返回</button>
                        <h2 style="margin:0;">👥 创建群聊</h2>
                    </div>
                    <div style="margin-bottom:16px;">
                        <label style="display:block;margin-bottom:6px;font-weight:500;">群名称</label>
                        <input type="text" id="group-name" placeholder="输入群聊名称" class="form-input" style="width:100%;">
                    </div>
                    <div style="margin-bottom:20px;">
                        <label style="display:block;margin-bottom:6px;font-weight:500;">选择成员（至少1位）</label>
                        ${friends.length === 0 ? '<div style="color:#999;padding:12px;">你还没有私聊好友，先去添加好友吧</div>' : ''}
                        <div style="display:flex;flex-direction:column;gap:8px;">
                            ${friends.map(f => `
                                <label style="display:flex;align-items:center;gap:10px;padding:10px;border:1px solid #e5e7eb;border-radius:8px;cursor:pointer;">
                                    <input type="checkbox" value="${f.id}" class="group-member-check" style="width:18px;height:18px;">
                                    <div class="chat-avatar" style="width:32px;height:32px;font-size:1rem;">${this.renderAvatarContent(f.avatar)}</div>
                                    <span>${f.nickname || f.id}</span>
                                </label>
                            `).join('')}
                        </div>
                    </div>
                    <button onclick="app.doCreateGroup()" class="btn btn-primary btn-block" ${friends.length === 0 ? 'disabled style="opacity:0.5;"' : ''}>✅ 创建群聊</button>
                </div>
            </div>
        `;
    }

    doCreateGroup() {
        const name = document.getElementById('group-name').value.trim();
        const checkboxes = document.querySelectorAll('.group-member-check:checked');
        const memberIds = Array.from(checkboxes).map(cb => cb.value);
        this.createGroup(name, memberIds);
    }

    renderGroupChat(groupId) {
        const group = this.groups.find(g => g.id === groupId);
        if (!group) {
            return `<div class="empty-state" style="margin-top:100px;"><h3>群聊不存在</h3><button onclick="app.router('chats')" class="btn btn-primary">返回</button></div>`;
        }
        const isMember = group.members.includes(this.currentUser.id) && !group.removedMembers?.includes(this.currentUser.id);
        if (!isMember) {
            return `<div class="empty-state" style="margin-top:100px;"><h3>你已不在该群聊中</h3><button onclick="app.router('chats')" class="btn btn-primary">返回</button></div>`;
        }
        this.currentGroupId = groupId;
        const msgs = (this.groupMessages[groupId] || []).sort((a, b) => a.timestamp - b.timestamp);
        let lastDate = '';
        const messagesHtml = msgs.map(msg => {
            const isMe = msg.from === this.currentUser.id;
            const fromUser = this.users[msg.from] || { nickname: '未知用户', avatar: '👤' };
            const d = new Date(msg.timestamp);
            const dateStr = d.toLocaleDateString('zh-CN');
            const timeStr = d.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
            let dateDivider = '';
            if (dateStr !== lastDate) {
                dateDivider = `<div class="chat-date-divider"><span>${dateStr}</span></div>`;
                lastDate = dateStr;
            }
            let mediaHtml = '';
            if (msg.mediaType && msg.media) {
                if (msg.mediaType === 'image') {
                    mediaHtml = `<div style="margin-top:8px;"><img src="${msg.media}" style="max-width:200px;max-height:200px;border-radius:12px;display:block;cursor:zoom-in;" onclick="app.showImageModal('${msg.media}')"></div>`;
                } else if (msg.mediaType === 'video') {
                    mediaHtml = `<div style="margin-top:8px;"><video src="${msg.media}" controls muted preload="metadata" style="max-width:260px;max-height:240px;border-radius:12px;display:block;"></video></div>`;
                } else if (msg.mediaType === 'voice') {
                    const dur = msg.duration ? Math.round(msg.duration) + ' 秒' : '语音';
                    mediaHtml = `<div style="margin-top:8px;display:flex;align-items:center;gap:8px;min-width:160px;">
                        <button onclick="this.nextElementSibling.play()" style="background:transparent;border:none;font-size:1.3rem;cursor:pointer;">▶️</button>
                        <audio src="${msg.media}" style="max-width:200px;"></audio>
                        <span style="font-size:0.85rem;opacity:0.7;">🎙️ ${dur}</span>
                    </div>`;
                }
            }
            const textHtml = msg.content ? `<div class="msg-content">${this.escapeHtml(msg.content)}</div>` : '';
            return `
                ${dateDivider}
                <div class="chat-message ${isMe ? 'mine' : 'theirs'}">
                    ${!isMe ? `<div class="msg-avatar">${this.renderAvatarContent(fromUser.avatar)}</div>` : ''}
                    <div class="msg-bubble">
                        ${!isMe ? `<div style="font-size:0.75rem;opacity:0.6;margin-bottom:2px;">${fromUser.nickname}</div>` : ''}
                        ${textHtml}
                        ${mediaHtml}
                        <div class="msg-time">${timeStr}</div>
                    </div>
                </div>
            `;
        }).join('');
        return `
            <div class="chat-page">
                <div class="chat-header">
                    <button class="back-btn" onclick="app.router('chats')">←</button>
                    <div class="chat-header-avatar" style="font-size:1.5rem;">${group.avatar}</div>
                    <div class="chat-header-info">
                        <h3>${group.name}</h3>
                        <div style="font-size:0.8rem;opacity:0.7;">${group.members.filter(m => !group.removedMembers?.includes(m)).length} 人</div>
                    </div>
                    <button onclick="app.leaveGroup('${groupId}')" class="btn btn-small" style="margin-left:auto;background:#fee2e2;color:#dc2626;">退出</button>
                </div>
                <div class="chat-messages" id="group-chat-messages">
                    ${msgs.length === 0 ? `
                        <div class="empty-state" style="margin-top:100px;">
                            <div class="empty-icon">👋</div>
                            <p>还没有消息，发送第一条消息开始群聊吧！</p>
                        </div>
                    ` : messagesHtml}
                </div>
                <div class="chat-input-bar">
                    <button class="chat-media-btn" onclick="app.submitGroupChatFile('${groupId}')" title="发送图片/视频">📎</button>
                    <input type="text" id="group-chat-input" placeholder="输入消息..." onkeydown="if(event.key==='Enter'){event.preventDefault();app.submitGroupChatMessage('${groupId}');}" autocomplete="off">
                    <button class="btn btn-primary" onclick="app.submitGroupChatMessage('${groupId}')">发送</button>
                </div>
            </div>
        `;
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}

// 初始化应用
window.app = new ForumApp();

// 点击外部关闭下拉菜单
document.addEventListener('click', (e) => {
    if (!e.target.closest('.user-menu')) {
        const dropdown = document.getElementById('user-dropdown');
        if (dropdown) dropdown.classList.remove('show');
    }
});

// 处理URL参数
const urlParams = new URLSearchParams(window.location.search);
const sharedPost = urlParams.get('post');
if (sharedPost) {
    setTimeout(() => window.app.router('post', { postId: sharedPost }), 500);
}
