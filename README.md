# wechat history export

微信聊天记录导出为csv。

**目前仅在macOS上测试了**，不确定其它平台的聊天记录也在AES加密的sqlite3数据库中，
如果一样的话应该也可以使用这个方法，欢迎PR。

## demo

提供了一个demo，在`demo.py`，
这**不是开箱即用**的脚本，你可能需要自己动手。

或许后续你可以使用[pysqlcipher](https://github.com/leapcode/pysqlcipher)。

## 一些信息

假设微信信息存放在目录`wechat_root=~/Library/Containers/com.tencent.xinWeChat/Data/Library/Application Support/com.tencent.xinWeChat/xxx_version/xxx`下的相关AES加密的sqlite3数据库文件中：

- 联系人: `wechat_root/Contanct/wccontact_new2.db`
- 群聊: `wechat_root/Group/group_new.db`
- 聊天记录: `wechat_root/Message/msg_xxx.db`

想办法解密数据库文件就可以获取到想要的信息了。

## 依赖

- [sqlcipher](https://github.com/sqlcipher/sqlcipher)

## AES Key 获取

### macOS

- 打开微信客户端, **先不要登录**
- `lldb -p $(pgrep WeChat)`
    - `br set -n sqlite3_key`
    - `continue`
- 登录
- 返回lldb `memory read --size 1 --format x --count 32 $rsi`
- 上边打印的即为256-bit的aes key

### 其它

网上应该能找到一些其它的方法，如果你试了可以work，欢迎PR。

## 数据库文件解密

```sh
# open encrypted database by sqlcipher
sqlcipher wccontact_new2.db

# set decryption parameter in sqlcipher
PRAGMA key = "x'your_aes_key_here'";
PRAGMA cipher_page_size = 1024;
PRAGMA kdf_iter = '64000';
PRAGMA cipher_kdf_algorithm = PBKDF2_HMAC_SHA1;
PRAGMA cipher_hmac_algorithm = HMAC_SHA1;

# check decryption succeed or not
SELECT COUNT(*) FROM sqlite_master;

# dump decrypted database to xxx_dec.db
ATTACH DATABASE 'xxx_dec.db' AS plaintext KEY '';
SELECT sqlcipher_export('plaintext');
DETACH DATABASE plaintext;
```

## Write Ahead Log合并

使用上边的方式导出解密后的数据库，可能有些最新的数据不在数据库中，
后来发现微信启用了[wal](https://www.sqlite.org/wal.html)，
可以通过checkpoint合并，以`msg_0.db`为例：

```sh
mkdir wd
cp msg_0.db msg_0.db-shm msg_0.db-wal wd/
cd wd

# open encrypted database by sqlcipher
sqlcipher msg_0.db

# set decryption parameter in sqlcipher
PRAGMA key = "x'your_aes_key_here'";
PRAGMA cipher_page_size = 1024;
PRAGMA kdf_iter = '64000';
PRAGMA cipher_kdf_algorithm = PBKDF2_HMAC_SHA1;
PRAGMA cipher_hmac_algorithm = HMAC_SHA1;

# check decryption succeed or not
# SELECT COUNT(*) FROM sqlite_master;

# merge wal
PRAGMA wal_checkpoint;

# dump decrypted database to xxx_dec.db
ATTACH DATABASE 'xxx_dec.db' AS plaintext KEY '';
SELECT sqlcipher_export('plaintext');
DETACH DATABASE plaintext;
```
