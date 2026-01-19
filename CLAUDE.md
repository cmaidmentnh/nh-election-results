# NH Election Results - Claude Reference

## Deployment

```bash
# Deploy to production
git push && ssh root@138.197.20.97 "cd /opt/nh-election-results && git pull && systemctl restart nh-election-results"
```

## Server

- Host: 138.197.20.97
- Path: /opt/nh-election-results
- Service: nh-election-results
- URL: elections.nhhouse.gop

## Database

- Local: nh_elections.db
- NEVER delete data without explicit user command
- Back up before major changes: `cp nh_elections.db nh_elections.db.backup_$(date +%Y%m%d)`

## Key Files

- `app.py` - Flask routes
- `analysis.py` - Data analysis functions
- `queries.py` - Database queries
- `import_missing_towns.py` - Re-import town data from XLS files

## Notes

- Multi-member districts: Use MAX (top vote-getter) not SUM for margin calculations
- Ties: If candidates tie at cutoff, neither wins that seat
- Towns sharing county names: Hillsborough, Carroll, Grafton, Strafford, Sullivan, Merrimack
