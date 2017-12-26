import { EventEmitter } from 'events';
import * as mysql from 'mysql2/promise';
import { Db as HirelingDb, DbEvent } from 'hireling/db';
import { JobId, JobAttr } from 'hireling/job';
import { WorkerId } from 'hireling/worker';
import { Serializer } from 'hireling/serializer';

interface MysqlAttr {
}

// TODO: use @types package
interface MysqlConnection extends EventEmitter {
  end:     (...args: any[]) => Promise<any>;
  query:   (query: string) => any;
  execute: (query: string, args?: any[]) => Promise<[MysqlAttr[], any]>;
  release: () => void;
}

interface MysqlPool extends EventEmitter {
  end:     (...args: any[]) => Promise<any>;
  query:   (query: string) => any;
  execute: (query: string, args?: any[]) => Promise<[MysqlAttr[], any]>;
}

export const MYSQL_DEFS = {
  host:     'localhost',
  port:     3306,
  database: 'hireling',
  user:     'hireling',
  password: 'hireling',
  table:    'jobs',
  opts:     ({} as any) as object // pass-through client options, merged in
};

export type MysqlOpt = typeof MYSQL_DEFS;

export class MysqlEngine extends HirelingDb {
  private pool: MysqlPool;
  private readonly dbc: MysqlOpt;
  private readonly dbtable: string;
  private readonly sproc = 'reserve_sproc';

  constructor(opt?: Partial<MysqlOpt>) {
    super();

    this.log.debug('db created (mysql)');

    this.dbc = { ...MYSQL_DEFS, ...opt };
    this.dbtable = `\`${this.dbc.database}\`.\`${this.dbc.table}\``;
  }

  open() {
    const pool = mysql.createPool({
      host:     this.dbc.host,
      port:     this.dbc.port,
      user:     this.dbc.user,
      password: this.dbc.password,
      database: this.dbc.database,
      ...this.dbc.opts
    });

    pool.getConnection().then(async (conn: MysqlConnection) => {
      this.log.debug('opened');

      this.pool = pool;

      conn.release();

      conn.on('error', (err) => {
        this.log.error('db connection error', err);

        this.event(DbEvent.close, err);
      });

      await this.initSchema();

      this.event(DbEvent.open);
    })
    .catch((err: Error) => {
      this.log.error('could not connect to db', err);

      this.event(DbEvent.close, err);
    });
  }

  close(force = false) {
    this.log.warn(`closing - ${force ? 'forced' : 'graceful'}`);

    this.pool.end()
      .then(() => {
        this.log.debug('closed');

        this.event(DbEvent.close);
      })
      .catch((err: Error) => {
        this.log.debug('close error', err);

        this.event(DbEvent.close, err);
      });
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

    await this.pool.execute(insert, vals);
  }

  async getById(id: JobId) {
    this.log.debug('get job by id');

    const [rows] = await this.pool.execute(
      `SELECT * FROM ${this.dbtable} WHERE \`id\` = ?`, [id]
    );

    const result: MysqlAttr|null = rows[0] || null;

    return result ? MysqlEngine.fromMysql<JobAttr>(result) : null;
  }

  async get(query: Partial<JobAttr>) {
    this.log.debug('search jobs');

    const where = Object.keys(query).map(k => `\`${k}\` = ?`).join(' AND ');

    const [rows] = await this.pool.execute(
      `SELECT * FROM ${this.dbtable} WHERE ${where}`,
      Object.values(query)
    );

    const results: MysqlAttr[] = rows;

    return results.map(r => MysqlEngine.fromMysql<JobAttr>(r));
  }

  async reserve(wId: WorkerId) {
    this.log.debug(`atomic reserve job ${wId}`);

    // call stored procedure and unwrap nested results
    const [[[row]]] = await this.pool.query(`CALL ${this.sproc}()`);

    const result: MysqlAttr|null = row || null;

    return result ? MysqlEngine.fromMysql<JobAttr>(result) : null;
  }

  async updateById(id: JobId, values: Partial<JobAttr>) {
    this.log.debug('update job');

    const [rows] = await this.pool.execute(
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

    const [rows] = await this.pool.execute(
      `DELETE FROM ${this.dbtable} WHERE \`id\` = ?`, [id]
    ) as any;

    if (!rows || !rows.affectedRows) {
      throw new Error(`deleted ${rows.affectedRows || 0} jobs instead of 1`);
    }

    return true;
  }

  async remove(query: Partial<JobAttr>) {
    this.log.debug('remove jobs');

    const where = Object.keys(query).map(k => `\`${k}\` = ?`).join(' AND ');

    const [rows] = await this.pool.execute(
      `DELETE FROM ${this.dbtable} WHERE ${where}`,
      Object.values(query)
    ) as any;

    return rows && rows.affectedRows || 0;
  }

  async clear() {
    const [rows] = await this.pool.execute(
      // `TRUNCATE TABLE ${this.dbtable}`
      `DELETE FROM ${this.dbtable}`
    ) as any;

    return rows && rows.affectedRows || 0;
  }

  private async initSchema() {
    await this.pool.execute(
      `CREATE TABLE IF NOT EXISTS ${this.dbtable} (
        \`id\` CHAR(36) NOT NULL,
        \`workerid\` CHAR(36) NULL,
        \`name\` VARCHAR(100) NULL,
        \`created\` DATETIME NOT NULL,
        \`expires\` DATETIME NULL,
        \`expirems\` INT NULL,
        \`stalls\` DATETIME NULL,
        \`stallms\` INT NULL,
        \`status\` ENUM('ready', 'processing', 'done', 'failed') NOT NULL,
        \`retryx\` INT NOT NULL,
        \`retries\` INT NOT NULL,
        \`data\` LONGTEXT NULL,
        PRIMARY KEY (\`id\`),
        UNIQUE INDEX \`id_UNIQUE\` (\`id\` ASC)
      )`
    );

    const indexes = [
      `ALTER TABLE ${this.dbtable} ADD INDEX \`status\` (\`status\` ASC)`
    ];

    for (const i of indexes) {
      try {
        await this.pool.execute(i);
      }
      catch (err) {
        // ignore existing index errors
      }
    }

    await this.pool.query(`DROP PROCEDURE IF EXISTS \`${this.sproc}\``);

    await this.pool.query(
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
