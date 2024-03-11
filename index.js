#!/usr/bin/env node

import { Command } from 'commander';
import { check, updateOne, updateAll } from './lib.js';
const program = new Command();

program
  .name('bus-lader-db')
  .description('CLI tool to setup and update databases for bus-lader');

program
  .command('check')
  .description('check if update needed according to constants.json.')
  .action(() => {
    check();
  });
program
  .command('update [companyID]')
  .description('update specified company data. If no ID specifed, update all.')
  .action((companyId) => {
    if (companyId) {
      updateOne(companyId);
    } else {
      updateAll();
    }
  });

if (process.argv.length == 2) {
  process.argv.push('-h');
}

program.parse();
