# User 用户与鉴权

提供注册、登录、Token 会话和账号禁用。密码使用 OpenSSL PBKDF2-HMAC-SHA256、随机盐和固定工作因子保存，不再使用 `std::hash`；用户基本资料通过 LocalKV/WAL 恢复。

默认模式使用进程内 Session，便于零依赖运行；配置 Redis 后，Session、用户索引和用户凭证会写入共享 Redis，支持双实例间复用 Token。生产环境仍需补充 Refresh Token、权限中心和 TLS。
