/*
 * Copyright (c) 2023, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
/* eslint-disable camelcase */
/* eslint-disable no-console */
/* eslint-disable no-await-in-loop */
/* eslint-disable class-methods-use-this */
/* eslint-disable no-console */
/* eslint-disable @typescript-eslint/no-unused-vars */

import { existsSync, renameSync, readFileSync, writeFileSync, appendFileSync } from 'node:fs';

import { SfCommand, Flags } from '@salesforce/sf-plugins-core';
import { Messages } from '@salesforce/core';
Messages.importMessagesDirectoryFromMetaUrl(import.meta.url);
const messages = Messages.loadMessages('@salesforce/plugin-fieldarchive', 'copy.object');

export type CopyObjectResult = {
  path: string;
};

export default class CopyObject extends SfCommand<CopyObjectResult> {
  public static readonly summary = messages.getMessage('summary');
  public static readonly description = messages.getMessage('description');
  public static readonly examples = messages.getMessages('examples');

  public static readonly flags = {
    name: Flags.string({
      summary: messages.getMessage('flags.name.summary'),
      description: messages.getMessage('flags.name.description'),
      char: 'n',
      required: true,
    }),
    'target-org': Flags.requiredOrg(),
    config: Flags.file({
      summary: messages.getMessage('flags.config.summary'),
      char: 'c',
      required: true,
      exists: true,
    }),
  };

  public async run(): Promise<CopyObjectResult> {
    type FieldArchive = {
      ULTEST__ParentId__c: string;
      ULTEST__Field__c: string;
      ULTEST__CreatedDate__c: Date;
      ULTEST__DataType__c: string;
      ULTEST__LegacyId__c: string;
      ULTEST__CreatedById__c: string;
    };
    type ObjectMap = {
      source: string;
      target: string;
    };
    const MapObject: Map<string, string> = new Map<string, string>();
    const { flags } = await this.parse(CopyObject);

    const file = readFileSync(flags['config']);
    // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
    const a: ObjectMap[] = JSON.parse(file.toString());
    for (const f of a) MapObject.set(f['source'], f['target']);

    const con = flags['target-org'].getConnection('58.0');
    let parentId: string;
    const source: string = flags['name'];
    const target: string = MapObject.get(source) as string;
    if (target === undefined) {
      this.log('No target defined for this source - Check config.log');
      this.exit();
    } else this.log('Moving object: ' + source + 'History to ' + target);

    const custom: boolean = (await con.sobject(source).describe()).custom;
    if (custom) parentId = 'ParentId';
    else parentId = source + 'Id';

    this.log('Lookup field: ' + parentId);

    const q: string =
      'select ' + parentId + ',Id, CreatedDate, Datatype, CreatedById, Field from ' + source + 'History';
    let qr = await con.query(q);
    this.log('size of object: ' + qr.totalSize);
    let allDone: boolean = false;
    // eslint-disable-next-line no-constant-condition
    while (!allDone) {
      let blockDone: boolean = false;
      while (!blockDone) {
        const l: Map<number, []> = new Map<number, []>();
        for (let i = 0; i < 10; i++) l.set(i, []);

        let blockCount: number = 0;
        const res = [];
        let t: FieldArchive[] = [];
        for (const f of qr.records) {
          t = l.get(blockCount) as FieldArchive[];

          t.push({
            ULTEST__ParentId__c: f[parentId] as string,
            ULTEST__Field__c: f['Field'] as string,
            ULTEST__DataType__c: f['Datatype'] as string,
            ULTEST__CreatedDate__c: f['CreatedDate'] as Date,
            ULTEST__LegacyId__c: f['Id'] as string,
            ULTEST__CreatedById__c: f['CreatedById'] as string,
          });

          if (t.length === 200) {
            this.log('insert');
            res[blockCount] = con.sobject(target).insert(t);
            blockCount++;
            this.log('insert done');
          }
        }
        if (t.length > 0) {
          res[blockCount] = con.sobject(target).insert(t);
          blockCount++;
        }
        // eslint-disable-next-line no-await-in-loop
        await Promise.all(res);
        this.log('Inserted ' + blockCount + ' blocks');

        // Make sure that all inserst worked

        for (let i = 0; i < blockCount; i++) {
          let blockRetry = true;
          while (blockRetry) {
            blockRetry = false;

            // eslint-disable-next-line no-await-in-loop
            for (const f of await res[i]) {
              if (!f.success) blockRetry = true;
            }
            // eslint-disable-next-line no-await-in-loop
            if (blockRetry) res[i] = await con.sobject(target).insert(l.get(i) as FieldArchive[]);
            else this.log('block ' + i + ' OK');
          }
        }
        // eslint-disable-next-line no-await-in-loop
        // const res = await con.sobject('ULTEST__FieldArchive__b').insert(l);
        blockDone = true;
      }
      // eslint-disable-next-line no-await-in-loop
      if (!qr.done) {
        // eslint-disable-next-line no-await-in-loop
        qr = await con.query(qr.nextRecordsUrl as string);
        this.log(qr.nextRecordsUrl);
      } else allDone = true;
    }

    //    const job = con.bulk2.createJob({operation:'insert', object: 'ULTEST__FieldArchive__b'});
    //    await job.open();
    //   await job.uploadData(l);
    //  await job.close();

    // const res = await con.sobject('ULTEST__FieldArchive__b').insert(l);
    // for (const f of res) {
    //  console.log (f);
    // }

    this.log(`Completed ${flags.name}`);
    return {
      path: '/Users/ksmeets/Local/Projects/plugin-fieldarchive/src/commands/copy/object.ts',
    };
  }
}
