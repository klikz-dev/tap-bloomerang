## Tap Specific Caveats:
- API returns duplicate records for interactions and transactions tables, tap will find these duplicate records and delete them as a result the Jenkins counts in log sometimes won’t match the record counts in Bytespree’s Studio ( This is added after consulting developer for this Tap)
- No “Known Limitations" provided for this Tap
- No caveats for test methods are provided by developer. 

## Automated Test Coverage
- [x] Retry logic
  - [x] HTTP errors
  - [x] Empty result set
- [x] Exception handling

## Manual Testing

### Set up connector in Bytespree
- [n/a] Verify Oauth success redirects back to Bytespree
- [n/a] Verify Oauth failure (invalid credentials) works as expected, not breaking app flow
- [x] Tap tests credentials for validity
- [x] Tap returns tables appropriately
- [n/a] Tap returns columns for table settings
- [n/a] Secondary options are populated
- [n/a] Verify conditional logic works within connector settings (will require looking @ definition.json)
- [x] Make sure all fields that are required for authentication are, indeed, required
- [x] Ensure all settings have appropriate description
- [x] Ensure all settings have proper data type (e.g. a checkbox vs a textbox)
- [x] "Known Limitations" is populated and spellchecked
- [x] "Getting Started" is populated and spellchecked
- [x] Make sure tap settings appear in a logical order (e.g. user before password or where conditional logic is applied)
- [x] If tables aren't pre-populated, ensure user can add tables manually by typing them in
- [ ] (FUTURE) Sensitive information should be hidden by default
- [ ] (FUTURE) Test di_partner_integration_tables.minimum_sync_date works as expected (todo: find tap that uses this, maybe rzrs edge)
- [ ] (FUTURE) Ensure 15 minute & hourly sync is disabled if tap is full table replace

### Test Method
- [x] Valid credentials completes test method successfully
- [x] Invalid or incomplete credentials fails test method
- [x] Documented caveats for test methods are provided

### Build Method
- [x] Verify table was created successfully
- [x] Verify indexes were created
- [x] If unique indexes are not used, developer needs to explain why

### Sync Method
- [x] Appropriate column types are assigned
- [x] JSON data is in a JSON field
- [x] Spot check 10-15 records pulled in from sync (note: shuf -n 10 output.log --- expanded in Spot Checking)
- [x] Verify the counts match from Jenkins log matches records in Bytespree
- [x] Check for duplicate records, within reason
- [x] Verify columns added to source are added to columns in Bytespree database
- [x] When connector supports deleting records, ensure physical deletion occurs
