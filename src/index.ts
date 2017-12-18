import { EventEmitter } from 'events';
import * as mysql from 'mysql2/promise';
import { Db as HirelingDb, DbEvent } from 'hireling/db';
import { JobId, JobAttr, JobStatus } from 'hireling/job';
import { WorkerId } from 'hireling/worker';
import { Serializer } from 'hireling/serializer';

interface MysqlAttr {
}

// TODO: use @types package
interface MysqlConnection extends EventEmitter {
  end:     (...args: any[]) => Promise<any>;
  query:   (query: string) => any;
  execute: (query: string, args?: any[]) => Promise<[MysqlAttr[], any]>;
}

export const MYSQL_DEFS = {
  host:     'localhost',
  port:     3306,
  db:       'test',
  user:     'test',
  password: 'test',
  table:    'jobs'
};

export type MysqlOpt = typeof MYSQL_DEFS;

export class MysqlEngine extends HirelingDb {
  private connection: MysqlConnection;
  private readonly dbc: MysqlOpt;
  private readonly dbtable: string;
  private readonly sproc = 'atomicFindReady';

  constructor(opt?: Partial<MysqlOpt>) {
    super();

    this.log.debug('db created (mysql)');

    this.dbc = { ...MYSQL_DEFS, ...opt };
    this.dbtable = `\`${this.dbc.db}\`.\`${this.dbc.table}\``;
  }

  open() {
    mysql.createConnection({
      host:     this.dbc.host,
      user:     this.dbc.user,
      password: this.dbc.password,
      database: this.dbc.db
    })
    .then(async (conn: MysqlConnection) => {
      this.log.debug('opened');

      this.connection = conn;

      conn.on('error', (err) => {
        this.log.error('db connection error', err);

        this.event(DbEvent.close, err);
      });

      await this.createTable();

      this.event(DbEvent.open);
    })
    .catch((err: Error) => {
      this.log.error('could not connect to db', err);

      this.event(DbEvent.close, err);
    });
  }

  close(force = false) {
    this.log.warn(`closing - ${force ? 'forced' : 'graceful'}`);

    this.connection.end()
      .then(() => {
        this.log.debug('closed');

        this.event(DbEvent.close);
      })
      .catch((err: Error) => {
        this.log.debug('close error', err);

        this.event(DbEvent.close, err);
      });
  }

  async clear() {
    const [rows] = await this.connection.execute(
      // `TRUNCATE TABLE ${this.dbtable}`
      `DELETE FROM ${this.dbtable}`
    ) as any;

    return rows && rows.affectedRows || 0;
  }

  async add(job: JobAttr) {
    this.log.debug(`add job ${job.id}`);

    type Key = keyof JobAttr;
    const keys: Key[] = [];
    const vals: any = [];

    Object.entries(job).forEach(([key, val]) => {
      keys.push(key as Key);
      vals.push((key === 'data') ? Serializer.pack(val) : val);
    });

    const insert = [
      `INSERT INTO ${this.dbtable}`,
      `(${keys.map(k => `\`${k}\``).join(', ')})`,
      `VALUES (${vals.map(() => '?').join(', ')})`
    ].join(' ');

    await this.connection.execute(insert, vals);
  }

  async getById(id: JobId) {
    this.log.debug('get job by id');

    const [rows] = await this.connection.execute(
      `SELECT * FROM ${this.dbtable} WHERE \`id\` = ?`, [id]
    );

    const result: MysqlAttr|null = rows[0] || null;

    return result ? MysqlEngine.fromMysql<JobAttr>(result) : null;
  }

  async atomicFindReady(wId: WorkerId) {
    this.log.debug(`atomic find update job ${wId}`);

    // call stored procedure and unwrap nested results
    const [[[row]]] = await this.connection.query(`CALL ${this.sproc}()`);

    const result: MysqlAttr|null = row || null;

    return result ? MysqlEngine.fromMysql<JobAttr>(result) : null;
  }

  async updateById(id: JobId, values: Partial<JobAttr>) {
    this.log.debug('update job');

    const [rows] = await this.connection.execute(
      `UPDATE ${this.dbtable}
      SET ${Object.keys(values).map(v => `${v} = ?`).join(', ')}
      WHERE \`id\` = ?`,
      [...Object.values(values), id]
    ) as any;

    if (!rows || !rows.affectedRows) {
      throw new Error(`updated ${rows.affectedRows || 0} jobs instead of 1`);
    }
  }

  async removeById(id: JobId) {
    this.log.debug('remove job by id');

    const [rows] = await this.connection.execute(
      `DELETE FROM ${this.dbtable} WHERE \`id\` = ?`, [id]
    ) as any;

    if (!rows || !rows.affectedRows) {
      throw new Error(`deleted ${rows.affectedRows || 0} jobs instead of 1`);
    }

    return true;
  }

  async removeByStatus(status: JobStatus) {
    this.log.debug(`remove jobs by status [${status}]`);

    const [rows] = await this.connection.execute(
      `DELETE FROM ${this.dbtable} WHERE \`status\` = ?`, [status]
    ) as any;

    return rows && rows.affectedRows || 0;
  }

  async refreshExpired() {
    this.log.debug('refresh expired jobs');

    // const now = new Date().toISOString().slice(0, 19).replace('T', ' ');
    const now = new Date();

    const [rows] = await this.connection.execute(
      `UPDATE ${this.dbtable}
      SET
        status = 'ready',
        workerid = NULL,
        attempts = attempts + 1,
        expires = DATE_ADD(NOW(), INTERVAL (expirems / 1000) SECOND)
      WHERE \`status\` = ? AND expires <= ?`,
      ['processing', now]
    ) as any;

    return rows && rows.affectedRows || 0;
  }

  async refreshStalled() {
    this.log.debug('refresh stalled jobs');

    // const now = new Date().toISOString().slice(0, 19).replace('T', ' ');
    const now = new Date();

    const [rows] = await this.connection.execute(
      `UPDATE ${this.dbtable}
      SET
        status = 'ready',
        workerid = NULL,
        attempts = attempts + 1,
        stalls = DATE_ADD(NOW(), INTERVAL (stallms / 1000) SECOND)
      WHERE \`status\` = ? AND stalls <= ?`,
      ['processing', now]
    ) as any;

    return rows && rows.affectedRows || 0;
  }

  private async createTable() {
    const create = [
      `CREATE TABLE IF NOT EXISTS ${this.dbtable} (`,
        '`id` CHAR(36) NOT NULL,',
        '`workerid` CHAR(36) NULL,',
        '`name` VARCHAR(100) NULL,',
        '`created` DATETIME NOT NULL,',
        '`expires` DATETIME NULL,',
        '`expirems` INT NULL,',
        '`stalls` DATETIME NULL,',
        '`stallms` INT NULL,',
        "`status` ENUM('ready', 'processing', 'done', 'failed') NOT NULL,",
        '`attempts` INT NOT NULL,',
        '`data` LONGTEXT NULL,',
        'PRIMARY KEY (`id`),',
        'UNIQUE INDEX `id_UNIQUE` (`id` ASC)',
      ')'
    ].join(' ');

    await this.connection.execute(create);

    const indexes = [
      [
        `ALTER TABLE ${this.dbtable}`,
        'ADD INDEX `status_expires` (`status` ASC, `expires` ASC)'
      ].join(' '),
      [
        `ALTER TABLE ${this.dbtable}`,
        'ADD INDEX `status_stalls` (`status` ASC, `stalls` ASC)'
      ].join(' ')
    ];

    for (const i of indexes) {
      try {
        await this.connection.execute(i);
      }
      catch (err) {
        // ignore existing index errors
      }
    }

    await this.connection.query(`DROP PROCEDURE IF EXISTS \`${this.sproc}\``);

    await this.connection.query(
      `CREATE PROCEDURE \`${this.sproc}\`()
      BEGIN
        START TRANSACTION;

        SET @j_id := NULL;
        SELECT id INTO @j_id
        FROM ${this.dbtable}
        WHERE status = 'ready' LIMIT 1;

        IF (@j_id IS NOT NULL) THEN
          UPDATE ${this.dbtable} SET status = 'processing' WHERE id = @j_id;
        END IF;

        SELECT * FROM ${this.dbtable} WHERE id = @j_id;

        COMMIT;
      END
    `);
  }

  private static fromMysql<T>(mobj: any) {
    if (mobj.data) {
      mobj.data = Serializer.unpack(mobj.data);
    }

    return mobj as T;
  }
}
