# NH Election Results - Claude Reference

## Server Access

```bash
# SSH to production server
ssh root@138.197.20.97

# App directory
cd /opt/nh-election-results
```

## Deployment

```bash
# Commit, push, and deploy
cd /Users/chrismaidment/Desktop/Data-Elections/election_app
git add -A && git commit -m "message" && git push && ssh root@138.197.20.97 "cd /opt/nh-election-results && git pull && systemctl restart nh-election-results"
```

## Checking Logs

```bash
# Recent logs
ssh root@138.197.20.97 "journalctl -u nh-election-results -n 50 --no-pager"

# Follow logs live
ssh root@138.197.20.97 "journalctl -u nh-election-results -f"
```

## Service Management

```bash
# Restart
ssh root@138.197.20.97 "systemctl restart nh-election-results"

# Status
ssh root@138.197.20.97 "systemctl status nh-election-results"
```

## App Details

- **Server**: 138.197.20.97
- **App Path**: /opt/nh-election-results
- **Port**: 5006 (behind nginx/Cloudflare)
- **Service**: nh-election-results.service
- **Workers**: 2 gunicorn workers

## Database

- SQLite database: `nh_elections.db`
- Contains NH election results 2016-2024

## GeoJSON Files

Located in `static/data/`:
- `nh-towns.geojson` - 259 NH towns
- `nh-counties.geojson` - 10 NH counties
- `nh-house-base-districts.geojson` - State House base districts
- `nh-house-floterial-districts.geojson` - State House floterial districts
- `nh-senate-districts.geojson` - State Senate districts
- `nh-exec-council-districts.geojson` - Executive Council districts
- `nh-congress-districts.geojson` - Congressional districts

## Important Notes

- **NEVER delete data without explicit DELETE command from user**
- Multi-member districts: Use MAX (top vote-getter) not SUM for margin calculations
- Ties: If candidates tie at cutoff, neither wins that seat
- Towns sharing county names: Hillsborough, Carroll, Grafton, Strafford, Sullivan, Merrimack
