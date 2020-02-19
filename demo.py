#!/usr/bin/env python3

import sqlite3
import subprocess
import logging
import os
import shutil
import hashlib

# your wechat root directory here
wechat_root = os.path.expanduser('~/Library/Containers/com.tencent.xinWeChat/Data/Library/Application Support/com.tencent.xinWeChat/xxx_version/xxx')
db_dir = 'wechat_history_export_plain_dbs'
# your wechat key here
wechat_raw_key = 'your_aes_key_here'

class DAO(object):
    def __init__(self, db_name):
        self._db_name = db_name
        self._db = sqlite3.connect(os.path.join(db_dir, db_name))
        self._cursor = self._db.cursor()

    def __del__(self):
        self._cursor.close()

        self._db.commit()
        self._db.close()

    def get_cursor(self):
        return self._cursor

def copy_db_files():
    dirs = ['Message', 'Group', 'Contact']
    for d in dirs:
        for f in os.listdir(os.path.join(wechat_root, d)):
            if 'backup' in f or not 'db' in f: continue
            shutil.copyfile(os.path.join(wechat_root, d, f), os.path.join(db_dir, f))
    logging.debug('copying encrypted database done')

def get_merge_wal_and_decrypt_sql(db_name):
    db_name = db_name.split('.')[0]
    merge_wal_and_decrypt_sql_tpl = '''
PRAGMA key = "x'{}'";
PRAGMA cipher_page_size = 1024;
PRAGMA kdf_iter = '64000';
PRAGMA cipher_kdf_algorithm = PBKDF2_HMAC_SHA1;
PRAGMA cipher_hmac_algorithm = HMAC_SHA1;

PRAGMA wal_checkpoint;

ATTACH DATABASE '{}.db' AS plaintext KEY '';
SELECT sqlcipher_export('plaintext');
DETACH DATABASE plaintext;
'''
    return merge_wal_and_decrypt_sql_tpl.format(wechat_raw_key, db_name + '_dec')

# merge write ahead log and decrypt database
def merge_wal_and_decrypt_all():
    def merge_wal_and_decrypt(db_name):
        sql = get_merge_wal_and_decrypt_sql(db_name)
        p = subprocess.run(['sqlcipher', db_name], stdout=subprocess.PIPE,
                input=sql, cwd=db_dir, encoding='utf8')
        if p.returncode != 0: logging.exception(p.stdout)
    dbs = filter(lambda f: not 'shm' in f and not 'wal' in f, os.listdir(db_dir))
    logging.debug('begin to decrypt database to "{}" directory'.format(db_dir))
    for db in dbs: merge_wal_and_decrypt(db)
    logging.debug('decrypt database done')

# remark 是单人备注或者群聊名称,
# 是群聊名称时group=True, 否则为False
def get_chat_hash_by_remark(remark, group=False):
    db_name = 'wccontact_new2_dec.db'
    table_name = 'WCContact'
    filter_field = 'm_nsRemark'
    if group:
        db_name = 'group_new_dec.db'
        table_name = 'GroupContact'
        filter_field = 'nickname'

    contact = DAO(db_name)
    sql = 'select m_nsUsrName, nickname, m_nsRemark from {} where {}=?'.format(table_name, filter_field)
    contact.get_cursor().execute(sql, (remark, ))
    result = contact.get_cursor().fetchall()
    logging.debug('contact info: {}'.format(result))
    hl = hashlib.md5()

    for c in result:
        # 有人会在表里有两条记录，但是其中一条的m_nsUserName为空串
        if not c[0]: continue
        hl.update(c[0].encode(encoding='utf-8'))
        md5_sum = hl.hexdigest()
        logging.debug('chat hash of {} is: {}'.format(remark, md5_sum))
        return md5_sum

    logging.warning('find contact "{}" by remark failed'.format(remark))
    return None

def get_dbname_and_tablename_contains_chat_hash(chat_hash):
    dbs = filter(lambda d: 'msg' in d and 'dec' in d, os.listdir(db_dir))
    for db_name in dbs:
        db = DAO(db_name)
        db.get_cursor().execute('select name from sqlite_master where type="table" and name not like "sqlite_%"')
        tables = db.get_cursor().fetchall()
        for table in tables:
            table_name = table[0]
            this_hash = table_name.split('_')[1]
            if this_hash == chat_hash:
                logging.debug('chat hash "{}" found: database: "{}", table: "{}"'
                        .format(chat_hash, db_name, table_name))
                return [db_name, table_name]
    return None

def export_table(db_name, table_name, csv_name):
    export_cmd = """
.headers on
.mode csv
.output {}
SELECT * FROM {};
""".format(csv_name, table_name)
    p = subprocess.run(['sqlite3', os.path.join(db_dir, db_name)], stdout=subprocess.PIPE,
            input=export_cmd, encoding='utf8')
    if p.returncode != 0: logging.exception(p.stdout)
    logging.info('succeed to export chat history to: "{}"'.format(csv_name))

def export_chat_history_by_remark(remark, group=False):
    chat_hash = get_chat_hash_by_remark(remark, group=group)
    db_name, table_name = get_dbname_and_tablename_contains_chat_hash(chat_hash)
    if not db_name or not table_name:
        raise Exception('failed to find db or table of "{}"'.format(remark))

    export_table(db_name, table_name, remark + '.csv')

def remove_db_files():
    shutil.rmtree(db_dir)

def prepare_db_dir():
    if os.path.exists(db_dir):
        logging.debug('removing existing db_dir "{}"'.format(db_dir))
        shutil.rmtree(db_dir)
    else:
        os.mkdir(db_dir)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
        format='%(asctime)s %(filename)s: [%(levelname)s] %(message)s')

    prepare_db_dir()

    copy_db_files()
    merge_wal_and_decrypt_all()

    # demo usage
    export_chat_history_by_remark('xxx_联系人_备注', group=False)
    #  export_chat_history_by_remark('xxx_群聊_名称', group=True)

    # remove db file for privacy protection
    remove_db_files()
