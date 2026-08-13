#!/usr/bin/env python3
"""
NH Election Results Explorer
Insight-driven web app for exploring NH election data
"""

import os
import re
from datetime import datetime
from flask import Flask, render_template, jsonify, request, Response, make_response, redirect, url_for
import queries
import analysis
import census

app = Flask(__name__)

# Session cookie hardening.
#   SECURE   - never send the session cookie over an unencrypted connection.
#              Flask's default is False, which means one plain http:// request,
#              made before the redirect to https, leaks the cookie to anyone on
#              the network path. Whoever copies it is logged in as that user.
#   HTTPONLY - JavaScript cannot read it, so injected script cannot steal it.
#   SAMESITE - not sent when another site triggers a request here, which stops a
#              malicious page acting as a logged-in user.
app.config['SESSION_COOKIE_SECURE'] = True
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 86400  # 1 day cache for static files

@app.context_processor
def inject_datetime():
    return {'now': datetime.now, 'datetime': datetime, 'ga_id': os.environ.get('GA_MEASUREMENT_ID', '')}
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')

# Set up Flask-Login
from auth import auth_bp, login_manager
from admin import admin_bp
from entry import entry_bp

login_manager.init_app(app)
login_manager.login_view = 'auth.login'
login_manager.login_message = 'Please log in to access this page.'

# Register blueprints
app.register_blueprint(auth_bp)
app.register_blueprint(admin_bp)
app.register_blueprint(entry_bp)


@app.route('/google5dd33fe4f5c62882.html')
def google_verification():
    """Google Search Console verification."""
    return app.send_static_file('google5dd33fe4f5c62882.html')


@app.route('/manifest.json')
def manifest_json():
    """Web app manifest."""
    manifest = {
        "name": "NH Election Results",
        "short_name": "NH Elections",
        "description": "New Hampshire election results, analysis, and historical data",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#ffffff",
        "theme_color": "#1e3a5f",
        "icons": [
            {"src": "/static/img/favicon-192.png", "sizes": "192x192", "type": "image/png"},
            {"src": "/static/img/favicon-512.png", "sizes": "512x512", "type": "image/png"}
        ]
    }
    return jsonify(manifest)


@app.errorhandler(404)
def page_not_found(e):
    """Custom 404 page."""
    return render_template('404.html'), 404


@app.route('/robots.txt')
def robots_txt():
    """Serve robots.txt for search engines."""
    content = """User-agent: *
Allow: /
Disallow: /admin/
Disallow: /entry/
Disallow: /login
Disallow: /logout
Disallow: /api/

Sitemap: https://elections.nhhouse.gop/sitemap.xml
"""
    return Response(content, mimetype='text/plain')


@app.route('/sitemap.xml')
def sitemap_xml():
    """Dynamic sitemap for search engines."""
    import sqlite3
    conn = sqlite3.connect('nh_elections.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    base = 'https://elections.nhhouse.gop'
    urls = []

    # Static pages
    static_pages = [
        ('/', '1.0', 'weekly'),
        ('/map', '0.8', 'monthly'),
        ('/candidates', '0.7', 'monthly'),
        ('/stats', '0.7', 'monthly'),
        ('/deep-analysis', '0.7', 'monthly'),
        ('/turnout', '0.7', 'monthly'),
        ('/incumbents', '0.7', 'monthly'),
        ('/ticket-splitting', '0.6', 'monthly'),
        ('/compare', '0.6', 'monthly'),
        ('/trump-comparison', '0.6', 'monthly'),
        ('/redistricting', '0.6', 'monthly'),
    ]
    for path, priority, freq in static_pages:
        urls.append(f'<url><loc>{base}{path}</loc><priority>{priority}</priority><changefreq>{freq}</changefreq></url>')

    # District browser pages
    for office in ['State Representative', 'State Senator', 'Executive Councilor', 'Representative in Congress']:
        urls.append(f'<url><loc>{base}/districts?office={office.replace(" ", "+")}</loc><priority>0.7</priority><changefreq>monthly</changefreq></url>')

    # Office pages
    for office_slug in ['governor', 'us-senate', 'us-house', 'state-senate', 'state-house']:
        urls.append(f'<url><loc>{base}/office/{office_slug}</loc><priority>0.8</priority><changefreq>monthly</changefreq></url>')

    # Town pages
    cursor.execute("SELECT DISTINCT municipality FROM results ORDER BY municipality")
    for row in cursor.fetchall():
        town = row['municipality']
        urls.append(f'<url><loc>{base}/town/{town.replace(" ", "%20")}</loc><priority>0.6</priority><changefreq>monthly</changefreq></url>')

    # County pages
    cursor.execute("SELECT DISTINCT county FROM races WHERE county IS NOT NULL AND county != '' ORDER BY county")
    for row in cursor.fetchall():
        county = row['county']
        urls.append(f'<url><loc>{base}/county/{county.replace(" ", "%20")}</loc><priority>0.6</priority><changefreq>monthly</changefreq></url>')

    # District pages
    cursor.execute("""
        SELECT DISTINCT r.county, r.district FROM races r
        JOIN elections e ON r.election_id = e.id
        WHERE e.redistricting_cycle = '2022-2030' AND r.county IS NOT NULL
        ORDER BY r.county, CAST(r.district AS INTEGER)
    """)
    for row in cursor.fetchall():
        urls.append(f'<url><loc>{base}/district/{row["county"].replace(" ", "%20")}/{row["district"]}</loc><priority>0.5</priority><changefreq>monthly</changefreq></url>')

    conn.close()

    xml = '<?xml version="1.0" encoding="UTF-8"?>\n'
    xml += '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
    xml += '\n'.join(urls)
    xml += '\n</urlset>'

    resp = make_response(xml)
    resp.headers['Content-Type'] = 'application/xml'
    return resp


@app.route('/')
def index():
    """Dashboard with key statewide insights."""
    stats = queries.get_db_stats()
    statewide = analysis.get_statewide_trends()
    towns = queries.get_all_towns()
    counties = queries.get_all_counties()

    years = sorted(statewide.keys())
    latest_year = years[-1] if years else 2024
    prev_year = years[-2] if len(years) >= 2 else None

    # Get party control for latest year
    party_control = analysis.get_party_control(latest_year)

    # Calculate changes from previous election
    changes = {}
    if prev_year:
        prev_control = analysis.get_party_control(prev_year)
        for office in party_control:
            if office in prev_control:
                r_change = party_control[office]['R'] - prev_control[office]['R']
                changes[office] = {
                    'r_change': r_change,
                    'd_change': -r_change,
                    'prev_year': prev_year
                }

    # Get closest races and biggest shifts
    closest_races = analysis.get_closest_races(latest_year, limit=8)
    biggest_shifts = analysis.get_biggest_shifts(prev_year, latest_year, limit=8) if prev_year else []

    # Statewide demographics
    demographics = census.get_statewide_demographics()

    return render_template('index.html',
                         stats=stats,
                         statewide=statewide,
                         years=years,
                         latest_year=latest_year,
                         prev_year=prev_year,
                         party_control=party_control,
                         changes=changes,
                         closest_races=closest_races,
                         biggest_shifts=biggest_shifts,
                         towns=towns,
                         counties=counties,
                         demographics=demographics)


@app.route('/town/<name>')
def town(name):
    """Town detail page with insights."""
    summary = analysis.get_town_summary(name)
    if not summary:
        return f"Town '{name}' not found", 404

    # Get detailed results for the most recent year
    latest_year = summary['years'][-1]
    races = analysis.get_town_race_details(name, latest_year)

    # Get comparison to previous election if available
    comparison = None
    if len(summary['years']) >= 2:
        prev_year = summary['years'][-2]
        comparison = analysis.compare_years(name, prev_year, latest_year)

    # Get PVI data
    pvi = analysis.get_town_pvi(name)

    # Get key races grid and representation
    key_races = analysis.get_town_key_races(name)
    representation = analysis.get_town_representation(name)

    # Get demographics
    demographics = census.get_town_demographics(name)

    return render_template('town.html',
                         summary=summary,
                         races=races,
                         comparison=comparison,
                         pvi=pvi,
                         key_races=key_races,
                         representation=representation,
                         demographics=demographics)


@app.route('/town/<name>/<int:year>')
def town_year(name, year):
    """Town results for a specific year."""
    summary = analysis.get_town_summary(name)
    if not summary or year not in summary['years']:
        return f"No data for {name} in {year}", 404

    races = analysis.get_town_race_details(name, year)
    pvi = analysis.get_town_pvi(name)

    return render_template('town_year.html',
                         summary=summary,
                         year=year,
                         races=races,
                         pvi=pvi)


@app.route('/district/<county>/<district>')
def district(county, district):
    """District explorer page."""
    office = request.args.get('office', 'State Representative')
    info = queries.get_district_info(county, district, office)
    results = queries.get_district_results(county, district, office)

    # Get PVI data for competitiveness
    pvi = analysis.get_district_pvi(office, district, county)
    # the modelled rating is on the NH-relative basis (predicted State House
    # margin at an even statewide House vote); fall back to the margin
    # classifier for offices the model does not cover
    lean = pvi.get('rating') or analysis.classify_lean(pvi['current_pvi'])

    # Get POTUS and Governor results for this district
    topline = analysis.get_district_topline_races(office, district, county)

    # Get demographics for district towns
    demographics = census.get_district_demographics(info['towns']) if info and info.get('towns') else {}

    # Group by year and calculate insights
    by_year = {}
    for r in results:
        year = r['year']
        if year not in by_year:
            by_year[year] = {'seats': r['seats'], 'candidates': [], 'r_seats': 0, 'd_seats': 0, 'top_r': 0, 'top_d': 0}
        candidate = {
            'name': r['candidate'],
            'party': r['party'],
            'votes': r['total_votes'],
            'is_winner': r['is_winner']
        }
        by_year[year]['candidates'].append(candidate)
        # Track TOP vote-getter per party (for fair margin calculation in multi-member races)
        if r['party'] == 'Republican':
            by_year[year]['top_r'] = max(by_year[year]['top_r'], r['total_votes'])
        elif r['party'] == 'Democratic':
            by_year[year]['top_d'] = max(by_year[year]['top_d'], r['total_votes'])
        if r['is_winner']:
            if r['party'] == 'Republican':
                by_year[year]['r_seats'] += 1
            elif r['party'] == 'Democratic':
                by_year[year]['d_seats'] += 1

    # Calculate margins using TOP vote-getter per party (fair for multi-member races)
    for year, data in by_year.items():
        total = data['top_r'] + data['top_d']
        if total > 0:
            data['margin'] = round((data['top_r'] - data['top_d']) / total * 100, 1)
        else:
            data['margin'] = 0

    # Get town-level results for map coloring
    town_results = queries.get_district_town_results(county, district, office)

    return render_template('district.html',
                         info=info,
                         by_year=by_year,
                         pvi=pvi,
                         lean=lean,
                         topline=topline,
                         demographics=demographics,
                         town_results=town_results)


@app.route('/county/<name>')
def county(name):
    """County overview page."""
    summary = analysis.get_county_summary(name)
    if not summary:
        return f"County '{name}' not found", 404

    year = request.args.get('year', type=int)
    county_offices = analysis.get_county_office_races(name, year)
    towns = summary.get('towns') or []
    topline = analysis.topline_for_towns(towns)
    return render_template('county.html', summary=summary, county_offices=county_offices,
                           county_topline=topline, county_topline_years=sorted(topline, reverse=True),
                           county_pvi=analysis.pvi_for_towns(towns))


@app.route('/candidates')
def candidates():
    """Candidate search."""
    query = request.args.get('q', '')
    results = []
    if query:
        results = queries.search_candidates(query)
    return render_template('candidates.html', query=query, results=results)


# API endpoints for charts
@app.route('/api/town/<name>/chart')
def api_town_chart(name):
    """Chart data for town trends."""
    summary = analysis.get_town_summary(name)
    if not summary:
        return jsonify({'error': 'Town not found'}), 404

    years = summary['years']
    margins = [summary['margins_by_year'][y]['margin'] for y in years]

    return jsonify({
        'labels': years,
        'datasets': [{
            'label': 'R Margin %',
            'data': margins,
            'borderColor': '#e63946',
            'backgroundColor': 'rgba(230, 57, 70, 0.1)',
            'fill': True,
            'tension': 0.3
        }]
    })


@app.route('/api/statewide/chart')
def api_statewide_chart():
    """Chart data for statewide trends."""
    statewide = analysis.get_statewide_trends()
    years = sorted(statewide.keys())

    house_r = [statewide[y].get('State Representative', {}).get('R', 0) for y in years]
    house_d = [statewide[y].get('State Representative', {}).get('D', 0) for y in years]
    senate_r = [statewide[y].get('State Senator', {}).get('R', 0) for y in years]
    senate_d = [statewide[y].get('State Senator', {}).get('D', 0) for y in years]

    return jsonify({
        'house': {
            'labels': years,
            'datasets': [
                {'label': 'Republican', 'data': house_r, 'backgroundColor': '#e63946'},
                {'label': 'Democratic', 'data': house_d, 'backgroundColor': '#457b9d'}
            ]
        },
        'senate': {
            'labels': years,
            'datasets': [
                {'label': 'Republican', 'data': senate_r, 'backgroundColor': '#e63946'},
                {'label': 'Democratic', 'data': senate_d, 'backgroundColor': '#457b9d'}
            ]
        }
    })


@app.route('/api/town/<name>/pvi')
def api_town_pvi(name):
    """PVI chart data for a town."""
    pvi = analysis.get_town_pvi(name)
    if not pvi or not pvi['years']:
        return jsonify({'error': 'Town not found'}), 404

    years = pvi['years']
    pvi_values = [pvi['pvi_by_year'][y]['pvi'] for y in years if y in pvi['pvi_by_year']]

    return jsonify({
        'labels': years,
        'datasets': [{
            'label': 'PVI (R+)',
            'data': pvi_values,
            'borderColor': '#1e3a5f',
            'backgroundColor': 'rgba(30, 58, 95, 0.1)',
            'fill': True,
            'tension': 0.3
        }]
    })


@app.route('/api/towns')
def api_towns():
    """List all towns."""
    return jsonify(queries.get_all_towns())


@app.route('/api/districts/<county>')
def api_districts(county):
    """Districts in a county."""
    return jsonify(queries.get_districts_by_county(county))


@app.route('/api/statewide-districts')
def api_statewide_districts():
    """Get districts for statewide offices (State Senate, Exec Council, Congress)."""
    office = request.args.get('office', 'State Senator')
    return jsonify(queries.get_statewide_districts(office))


@app.route('/districts')
def districts_browser():
    """Browse all districts for an office, sorted by PVI."""
    office = request.args.get('office', 'State Senator')
    # County offices aren't legislative PVI districts — send them to their own office page.
    _rev = {v: k for k, v in OFFICE_SLUGS.items()}
    if office.startswith('County') or office.startswith('Register'):
        return redirect(url_for('office_detail', office_name=_rev.get(office, 'county-sheriff')))
    districts = analysis.get_all_districts_with_pvi(office)

    return render_template('districts.html',
                         office=office,
                         districts=districts)


@app.route('/statewide-district/<office>/<district>')
def statewide_district(office, district):
    """View for statewide district (State Senate, Exec Council, Congress)."""
    results = queries.get_statewide_district_results(office, district)
    info = {
        'office': office,
        'district': district,
        'seats': results[0]['seats'] if results else 1,
        'towns': queries.get_towns_in_statewide_district(office, district)
    }

    # Get PVI data for competitiveness
    pvi = analysis.get_district_pvi(office, district)
    lean = analysis.classify_lean(pvi['current_pvi'])

    # Get POTUS and Governor results for this district
    topline = analysis.get_district_topline_races(office, district)

    # Get demographics for district towns
    demographics = census.get_district_demographics(info['towns']) if info and info.get('towns') else {}

    # Group by year
    by_year = {}
    for r in results:
        year = r['year']
        if year not in by_year:
            by_year[year] = {'seats': r['seats'], 'candidates': [], 'r_seats': 0, 'd_seats': 0, 'top_r': 0, 'top_d': 0}
        by_year[year]['candidates'].append(r)
        # Track TOP vote-getter per party
        if r['party'] == 'Republican':
            by_year[year]['top_r'] = max(by_year[year]['top_r'], r['votes'])
        elif r['party'] == 'Democratic':
            by_year[year]['top_d'] = max(by_year[year]['top_d'], r['votes'])
        if r['is_winner']:
            if r['party'] == 'Republican':
                by_year[year]['r_seats'] += 1
            elif r['party'] == 'Democratic':
                by_year[year]['d_seats'] += 1

    # Calculate margins using TOP vote-getter per party
    for year, data in by_year.items():
        total = data['top_r'] + data['top_d']
        if total > 0:
            data['margin'] = round((data['top_r'] - data['top_d']) / total * 100, 1)
        else:
            data['margin'] = 0

    # Get town-level results for map coloring
    town_results = queries.get_statewide_district_town_results(office, district)

    return render_template('statewide_district.html',
                         info=info,
                         by_year=by_year,
                         pvi=pvi,
                         lean=lean,
                         topline=topline,
                         demographics=demographics,
                         town_results=town_results)


# ============== NEW FEATURE ROUTES ==============

@app.route('/turnout')
def turnout():
    """Turnout analysis page."""
    turnout_data = analysis.get_turnout_analysis()
    return render_template('turnout.html', data=turnout_data)


@app.route('/ticket-splitting')
def ticket_splitting():
    """Ticket splitting analysis page."""
    splitting_data = analysis.get_ticket_splitting_analysis()
    return render_template('ticket_splitting.html', data=splitting_data)


@app.route('/redistricting')
def redistricting():
    """Redistricting impact analysis."""
    impact_data = analysis.get_redistricting_impact()
    return render_template('redistricting.html', data=impact_data)


# slug -> official office name (statewide/legislative + county offices)
OFFICE_SLUGS = {
    'president': 'President of the United States',
    'governor': 'Governor',
    'us-senate': 'United States Senator',
    'us-house': 'Representative in Congress',
    'state-senate': 'State Senator',
    'state-house': 'State Representative',
    'exec-council': 'Executive Councilor',
    'county-attorney': 'County Attorney',
    'county-sheriff': 'County Sheriff',
    'county-treasurer': 'County Treasurer',
    'county-commissioner': 'County Commissioner',
    'register-of-deeds': 'Register of Deeds',
    'register-of-probate': 'Register of Probate',
}


@app.route('/office/<office_name>')
def office_detail(office_name):
    """Office-level results page."""
    office = OFFICE_SLUGS.get(office_name)
    if not office:
        return f"Office '{office_name}' not found", 404

    office_data = analysis.get_office_results(office)
    is_county = office.startswith('County') or office.startswith('Register')
    return render_template('office.html', office=office, office_name=office_name,
                           data=office_data, is_county=is_county)


@app.route('/office/<office_name>/<int:year>')
def office_year(office_name, year):
    """Office results for a specific year with all races."""
    office = OFFICE_SLUGS.get(office_name)
    if not office:
        return f"Office '{office_name}' not found", 404

    races = analysis.get_office_year_results(office, year)
    if not races:
        # An office simply not being on the ballot that cycle is a normal
        # absence, not an error: the President is not elected in a midterm and
        # a given Senate seat comes up once every six years. Returning 404 made
        # the year selector generate dead links. Show the years that do exist
        # instead.
        years = analysis.get_years_for_office(office)
        reason = None
        if office == 'President of the United States':
            reason = ('The President is elected every four years, so there is '
                      'no presidential race in a midterm year.')
        elif office == 'United States Senator':
            reason = ('New Hampshire\'s two Senate seats are contested on a '
                      'six-year cycle, so neither was on the ballot in '
                      f'{year}.')
        return render_template('no_race.html', office=office,
                               office_name=office_name, year=year,
                               years=years, reason=reason), 200

    # Group by county for State Rep
    by_county = {}
    for race in races:
        county = race.get('county') or 'Statewide'
        if county not in by_county:
            by_county[county] = []
        by_county[county].append(race)

    # Calculate totals
    total_r_seats = sum(1 for r in races for c in r['candidates'] if c['is_winner'] and c['party'] == 'Republican')
    total_d_seats = sum(1 for r in races for c in r['candidates'] if c['is_winner'] and c['party'] == 'Democratic')
    total_r_votes = sum(c['votes'] for r in races for c in r['candidates'] if c['party'] == 'Republican')
    total_d_votes = sum(c['votes'] for r in races for c in r['candidates'] if c['party'] == 'Democratic')

    is_county = office.startswith('County') or office.startswith('Register')
    return render_template('office_year.html',
                         office=office,
                         office_name=office_name,
                         year=year,
                         races=races,
                         by_county=by_county,
                         is_county=is_county,
                         total_r_seats=total_r_seats,
                         total_d_seats=total_d_seats,
                         total_r_votes=total_r_votes,
                         total_d_votes=total_d_votes)


@app.route('/office/<office_name>/<int:year>/<county>')
def county_race_detail(office_name, year, county):
    """Drill-down for a single county-office race (town-by-town + year-over-year).
    Commissioner districts pass ?district=N."""
    office = OFFICE_SLUGS.get(office_name)
    if not office:
        return f"Office '{office_name}' not found", 404
    district = request.args.get('district', '')
    data = analysis.get_county_race_detail(office, county, year, district)
    if not data:
        return f"No results for {office} in {county} {year}", 404
    return render_template('county_race.html', office_name=office_name, data=data)


@app.route('/incumbents')
def incumbents():
    """Incumbent tracker page."""
    incumbent_data = analysis.get_incumbent_analysis()
    return render_template('incumbents.html', data=incumbent_data)


@app.route('/trump-comparison')
def trump_comparison():
    """Compare R State Rep performance vs Trump by district."""
    data = analysis.get_trump_comparison()
    return render_template('trump_comparison.html',
                         underperformers=data['underperformers'],
                         outperformers=data['outperformers'],
                         avg_gap=data['avg_gap'])


@app.route('/compare')
def compare():
    """Head-to-head comparison tool."""
    type_ = request.args.get('type', 'town')  # town or district
    item1 = request.args.get('item1', '')
    item2 = request.args.get('item2', '')

    comparison = None
    if item1 and item2:
        if type_ == 'town':
            comparison = analysis.compare_towns(item1, item2)
        else:
            comparison = analysis.compare_districts(item1, item2)

    towns = queries.get_all_towns()
    return render_template('compare.html',
                         type=type_,
                         item1=item1,
                         item2=item2,
                         comparison=comparison,
                         towns=towns)


@app.route('/map')
def election_map():
    """Interactive election map."""
    year = request.args.get('year', 2024, type=int)
    metric = request.args.get('metric', 'pvi')  # pvi, margin, turnout
    return render_template('map.html', year=year, metric=metric)


@app.route('/api/map-data')
def api_map_data():
    """GeoJSON data for the map."""
    year = request.args.get('year', 2024, type=int)
    metric = request.args.get('metric', 'pvi')
    return jsonify(analysis.get_map_data(year, metric))


@app.route('/api/districts-map-data')
def api_districts_map_data():
    """District data for the map, keyed by district code (e.g., BE1, HI35)."""
    year = request.args.get('year')  # None for average, or specific year
    metric = request.args.get('metric', 'margin')  # 'margin' or 'pvi'
    return jsonify(analysis.get_districts_map_data(year=year, metric=metric))


# ---------------------------------------------------------------------------
# Contested-races map (2026 primary: districts where candidates > seats)
# ---------------------------------------------------------------------------
import sqlite3 as _sqlite3

COUNTY_ABBR = {
    "Belknap": "BE", "Carroll": "CA", "Cheshire": "CH", "Coos": "CO",
    "Grafton": "GR", "Hillsborough": "HI", "Merrimack": "ME",
    "Rockingham": "RO", "Strafford": "ST", "Sullivan": "SU",
}
ABBR_COUNTY = {v: k for k, v in COUNTY_ABBR.items()}

# url key -> (results office name, geographic level)
CONTESTED_OFFICES = {
    "governor":     ("Governor", "statewide"),
    "us-senate":    ("United States Senator", "statewide"),
    "us-house":     ("Representative in Congress", "district"),
    "exec-council": ("Executive Councilor", "district"),
    "state-senate": ("State Senator", "district"),
    "state-house":  ("State Representative", "house"),
    "delegate":     ("Delegate to the State Convention", "house"),
}


def _contested_db():
    conn = _sqlite3.connect('nh_elections.db')
    conn.row_factory = _sqlite3.Row
    return conn


def _district_code(level, county, district):
    """Map a race's (county, district) to the geojson feature code."""
    if level == "house":
        return f"{COUNTY_ABBR.get(county, county)}{district}"
    if level == "statewide":
        return "STATE"
    return str(district)


def _decode_code(level, code):
    """Inverse of _district_code -> (county, district)."""
    if level == "house":
        m = re.match(r"([A-Z]{2})(\d+)", code)
        if m:
            return ABBR_COUNTY.get(m.group(1), ""), m.group(2)
        return "", ""
    if level == "statewide":
        return "", ""
    return "", code


PRIMARY_DAY = datetime(2026, 9, 8).date()


@app.route('/contested')
def contested_map():
    """Public 2026 primary portal. Preview mode (who's running) until results
    start arriving on primary day, then it becomes live results + projections."""
    conn = _contested_db()
    has_results = conn.execute("""
        SELECT 1 FROM results res JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        WHERE e.year = 2026 AND e.election_type = 'state_primary' LIMIT 1""").fetchone() is not None
    conn.close()
    demo = request.args.get('demo') == '1'
    live = demo or datetime.now().date() >= PRIMARY_DAY or has_results
    return render_template('contested_map.html', live_mode=live, demo=demo)


@app.route('/api/contested/<office_key>')
def api_contested(office_key):
    """Per-district contested status for an office in the 2026 primary."""
    if office_key not in CONTESTED_OFFICES:
        return jsonify({'error': 'unknown office'}), 404
    office_name, level = CONTESTED_OFFICES[office_key]

    conn = _contested_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT e.party, r.county AS county, r.district AS district, r.seats AS seats,
               COUNT(rc.candidate_id) AS ncand,
               GROUP_CONCAT(c.name, '|') AS names
        FROM races r
        JOIN elections e   ON r.election_id = e.id
        JOIN offices o     ON r.office_id = o.id
        LEFT JOIN race_candidates rc
               ON rc.race_id = r.id AND rc.recruitment_filing_id > 0
        LEFT JOIN candidates c ON rc.candidate_id = c.id
        WHERE e.year = 2026 AND e.election_type = 'state_primary' AND o.name = ?
        GROUP BY e.party, r.county, r.district, r.seats
    """, (office_name,))

    districts = {}
    for row in cur.fetchall():
        code = _district_code(level, row['county'] or '', row['district'] or '')
        d = districts.setdefault(code, {
            'code': code, 'county': row['county'] or '', 'district': row['district'] or '',
            'seats': row['seats'], 'parties': {}, 'candidates': {}, 'contested': False,
        })
        party = (row['party'] or 'NP')[:1]  # R / D
        d['parties'][party] = row['ncand']
        d['candidates'][party] = row['names'].split('|') if row['names'] else []
        if row['ncand'] > (row['seats'] or 1):
            d['contested'] = True
    conn.close()

    return jsonify({'office': office_name, 'level': level, 'districts': districts})


def _precincts_for(cur, office_name, level, county, district):
    """The municipalities (towns/wards) that vote in a district."""
    if level == 'statewide':
        cur.execute("SELECT municipality FROM polling_places ORDER BY county, municipality")
    else:
        oid = cur.execute("SELECT id FROM offices WHERE name = ?", (office_name,)).fetchone()
        if not oid:
            return []
        cur.execute("""SELECT municipality FROM municipality_districts
                       WHERE office_id = ? AND county = ? AND district = ?
                       ORDER BY municipality""", (oid['id'], county, district))
    return [r['municipality'] for r in cur.fetchall()]


@app.route('/api/contested/<office_key>/precincts')
def api_contested_precincts(office_key):
    """Candidates + precinct (town/ward) list for one district."""
    if office_key not in CONTESTED_OFFICES:
        return jsonify({'error': 'unknown office'}), 404
    office_name, level = CONTESTED_OFFICES[office_key]
    code = request.args.get('code', '')
    county, district = _decode_code(level, code)

    conn = _contested_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT e.party, r.seats AS seats, c.name AS name
        FROM races r
        JOIN elections e   ON r.election_id = e.id
        JOIN offices o     ON r.office_id = o.id
        JOIN race_candidates rc ON rc.race_id = r.id AND rc.recruitment_filing_id > 0
        JOIN candidates c  ON rc.candidate_id = c.id
        WHERE e.year = 2026 AND e.election_type = 'state_primary' AND o.name = ?
          AND COALESCE(r.county,'') = ? AND COALESCE(r.district,'') = ?
        ORDER BY e.party, c.name
    """, (office_name, county, district))
    parties, seats = {}, 1
    for row in cur.fetchall():
        seats = row['seats']
        parties.setdefault(row['party'], []).append(row['name'])
    precincts = _precincts_for(cur, office_name, level, county, district)
    conn.close()

    towns = sorted({re.sub(r'\s+Ward\s+\d+\*?$', '', p).strip() for p in precincts})
    return jsonify({'office': office_name, 'code': code, 'county': county, 'district': district,
                    'seats': seats, 'parties': parties, 'precincts': precincts, 'towns': towns})


def _precinct_weights(cur, party_full, names):
    """Each precinct's expected share of the district's primary vote.

    Baseline = that party's 2024 state-primary turnout per precinct (best
    predictor of where this party's primary vote comes from); falls back to
    2024 general turnout (Governor), then to an equal share."""
    if not names:
        return {}
    qm = ",".join("?" * len(names))
    prim, genr = {}, {}
    cur.execute(f"""SELECT res.municipality AS m, SUM(res.votes) AS v
                    FROM results res JOIN races r ON res.race_id = r.id
                    JOIN elections e ON r.election_id = e.id
                    WHERE e.year = 2024 AND e.election_type = 'state_primary' AND e.party = ?
                      AND res.municipality IN ({qm}) GROUP BY res.municipality""",
                (party_full, *names))
    for row in cur.fetchall():
        prim[row['m']] = row['v']
    cur.execute(f"""SELECT res.municipality AS m, SUM(res.votes) AS v
                    FROM results res JOIN races r ON res.race_id = r.id
                    JOIN offices o ON r.office_id = o.id JOIN elections e ON r.election_id = e.id
                    WHERE e.year = 2024 AND e.election_type = 'general' AND o.name = 'Governor'
                      AND res.municipality IN ({qm}) GROUP BY res.municipality""",
                (*names,))
    for row in cur.fetchall():
        genr[row['m']] = row['v']
    raw = {n: (prim.get(n) or genr.get(n) or 1) for n in names}
    total = sum(raw.values()) or 1
    return {n: raw[n] / total for n in names}


# Uncertainty band for the call: outstanding expected-vote share ×
# (BASE + VOL_FACTOR × observed precinct-to-precinct volatility of the boundary
# margin). z = projected margin / band drives a 4-state call. Tuned so a ~30-pt
# lead with consistent returns is CALLED near 5% in, a ~10-pt lead reads "likely"
# mid-count, and a true squeaker stays "too close" until very late.
PROJ_BASE = 0.15
PROJ_VOL_FACTOR = 1.0
PROJ_MIN_IN = 0.005


import math as _math

PROJ_MIN_PRECINCTS = 3  # never CALL off fewer than this many reported precincts (unless math-clinched)
PROJ_SAMPLE = 0.12      # sampling uncertainty term ~ PROJ_SAMPLE / sqrt(reported precincts)


def _project(cand_list, seats, reported_weight, current_total, volatility=0.0,
             n_reported=0, n_total=0):
    """Decision-desk call from region-aware projected totals, with sampling guards
    so a single unrepresentative precinct (e.g. a candidate's home stronghold) can't
    trigger a call. Statuses: called / likely / leaning / too_close (+ awaiting)."""
    if reported_weight <= 0 or current_total <= 0:
        return {'status': 'awaiting', 'winners': [], 'expected_in': round(100 * reported_weight),
                'projected_total': None, 'margin': None}
    projected_total = current_total / reported_weight
    winners = [c['name'] for c in cand_list[:seats]]
    if len(cand_list) <= seats:
        return {'status': 'called', 'winners': winners, 'expected_in': round(100 * reported_weight),
                'projected_total': round(projected_total), 'margin': None}

    lead = cand_list[seats - 1]['projected'] or 0
    trail = cand_list[seats]['projected'] or 0
    margin = (lead - trail) / projected_total if projected_total else 0
    outstanding = max(0.0, 1.0 - reported_weight)
    # Band combines: how much vote is out × (base + observed volatility) PLUS a
    # sampling term that is large when few precincts have reported (so 1–2 precincts
    # can't look "certain" just because they happen to agree).
    sampling = PROJ_SAMPLE / _math.sqrt(max(1, n_reported))
    band = outstanding * (PROJ_BASE + PROJ_VOL_FACTOR * volatility) + sampling + 0.005
    gap_votes = cand_list[seats - 1]['votes'] - cand_list[seats]['votes']
    out_votes = max(0, projected_total - current_total)
    z = margin / band if band > 0 else 99
    enough = n_reported >= PROJ_MIN_PRECINCTS

    if gap_votes > out_votes:               # mathematically out of reach — always call
        status = 'called'
    elif outstanding < 0.01 and margin > 0:  # effectively all counted with a clear margin
        status = 'called'
    elif z >= 1.5 and enough:
        status = 'called'
    elif z >= 1.0:
        status = 'likely'
    elif z >= 0.5:
        status = 'leaning'
    else:
        status = 'too_close'
    if reported_weight < PROJ_MIN_IN and status != 'called':
        status = 'too_close'
    return {'status': status, 'winners': winners, 'margin': round(margin * 100, 1),
            'expected_in': round(100 * reported_weight), 'projected_total': round(projected_total)}


def _precinct_counties(cur, names):
    """municipality -> county, for region-aware projection."""
    if not names:
        return {}
    qm = ",".join("?" * len(names))
    cur.execute(f"SELECT municipality, county FROM polling_places WHERE municipality IN ({qm})", names)
    return {r['municipality']: (r['county'] or '') for r in cur.fetchall()}


def _demo_results(precs, cand_ids, race_id):
    """Deterministic but ARBITRARY simulated returns (~65% of precincts reporting)
    so the live portal can be demoed before election night. The lead candidate is
    rotated by race so it's clearly not a forecast — these are not real votes and
    not a prediction. Demo mode only; never stored."""
    n = len(cand_ids)
    base = [max(6, 100 - i * 20) for i in range(n)]            # descending strengths
    off = race_id % n                                          # rotate the leader per race
    strengths = {cid: base[(i + off) % n] for i, cid in enumerate(cand_ids)}
    by_prec = {}
    for pi, p in enumerate(precs[:max(1, int(len(precs) * 0.65))]):
        h = race_id * 31 + pi * 17
        by_prec[p] = {cid: max(1, strengths[cid] + ((h + i * 101) % 25) - 12 + (pi % 4))
                      for i, cid in enumerate(cand_ids)}
    return by_prec


def _compute_results(cur, office_name, level, county, district, party_full, party,
                     with_precincts=True, demo=False):
    """Full results + region-aware projection for one party's primary in a district."""
    race = cur.execute("""
        SELECT r.id AS id, r.seats AS seats FROM races r
        JOIN elections e ON r.election_id = e.id
        JOIN offices o   ON r.office_id = o.id
        WHERE e.year = 2026 AND e.election_type = 'state_primary' AND e.party = ?
          AND o.name = ? AND COALESCE(r.county,'') = ? AND COALESCE(r.district,'') = ?
    """, (party_full, office_name, county, district)).fetchone()
    if not race:
        return {'exists': False, 'office': office_name, 'party': party, 'candidates': [],
                'precincts': [], 'towns': [], 'seats': 1,
                'reporting': {'reported': 0, 'total': 0, 'pct': 0, 'expected_in': 0},
                'projection': {'status': 'awaiting', 'winners': [], 'expected_in': 0}}

    race_id, seats = race['id'], race['seats']
    cands = cur.execute("""SELECT c.id AS cid, c.name AS name FROM race_candidates rc
                           JOIN candidates c ON rc.candidate_id = c.id
                           WHERE rc.race_id = ? AND rc.recruitment_filing_id > 0
                           ORDER BY rc.ballot_order, c.name""", (race_id,)).fetchall()
    name_by_id = {c['cid']: c['name'] for c in cands}

    precs = _precincts_for(cur, office_name, level, county, district)
    if demo:
        by_prec = _demo_results(precs, list(name_by_id.keys()), race_id)
    else:
        res = cur.execute("SELECT municipality, candidate_id, votes FROM results WHERE race_id = ?", (race_id,)).fetchall()
        by_prec = {}
        for r in res:
            by_prec.setdefault(r['municipality'], {})[r['candidate_id']] = r['votes']

    overall = {cid: 0 for cid in name_by_id}
    precincts = []
    for p in precs:
        v = by_prec.get(p, {})
        for cid, val in v.items():
            if cid in overall:
                overall[cid] += val
        precincts.append({'name': p, 'reported': p in by_prec,
                          'votes': {name_by_id[cid]: val for cid, val in v.items() if cid in name_by_id}})
    reported_n = sum(1 for p in precincts if p['reported'])
    total = len(precincts)

    weights = _precinct_weights(cur, party_full, precs)
    pcounty = _precinct_counties(cur, precs)
    reported = {p['name'] for p in precincts if p['reported']}
    reported_weight = sum(weights.get(p, 0) for p in reported)
    current_total = sum(overall.values())
    projected_total = (current_total / reported_weight) if reported_weight > 0 else 0

    # Reported candidate shares, overall and per region (county), for region-aware
    # extrapolation of the outstanding vote.
    def _shares(votes, tot):
        return {cid: (votes.get(cid, 0) / tot if tot else 0) for cid in name_by_id}
    dist_shares = _shares(overall, current_total)
    region_votes, region_tot = {}, {}
    for p in reported:
        rg = pcounty.get(p, '')
        rv = region_votes.setdefault(rg, {cid: 0 for cid in name_by_id})
        for cid, val in by_prec.get(p, {}).items():
            if cid in rv:
                rv[cid] += val
                region_tot[rg] = region_tot.get(rg, 0) + val
    region_shares = {rg: _shares(rv, region_tot.get(rg, 0)) for rg, rv in region_votes.items()}

    # Projected totals: counted + each outstanding precinct's expected vote spread
    # by its region's reported shares (fallback district shares, then nothing).
    projected = {cid: overall[cid] for cid in name_by_id}
    for p in precincts:
        ev = weights.get(p['name'], 0) * projected_total
        p['expected'] = round(ev)
        if p['name'] in reported:
            continue
        rg = pcounty.get(p['name'], '')
        sh = region_shares.get(rg) if region_tot.get(rg, 0) > 0 else dist_shares
        for cid in name_by_id:
            projected[cid] += ev * sh[cid]

    cand_list = [{'name': name_by_id[cid], 'votes': overall[cid],
                  'projected': round(projected[cid]) if reported_weight > 0 else None}
                 for cid in name_by_id]
    cand_list.sort(key=lambda x: (-(x['projected'] if x['projected'] is not None else x['votes']), -x['votes']))

    # Volatility: weighted stddev of the boundary (last-winner vs first-loser)
    # margin share across reported precincts — consistent returns let us call early.
    volatility = 0.0
    if reported_weight > 0 and len(cand_list) > seats:
        name_to_id = {n: cid for cid, n in name_by_id.items()}
        bw_id = name_to_id.get(cand_list[seats - 1]['name'])
        bl_id = name_to_id.get(cand_list[seats]['name'])
        diffs = []
        for p in reported:
            pv = by_prec.get(p, {})
            pt = sum(pv.get(cid, 0) for cid in name_by_id)
            if pt > 0:
                diffs.append((weights.get(p, 0), (pv.get(bw_id, 0) - pv.get(bl_id, 0)) / pt))
        wsum = sum(w for w, _ in diffs)
        if wsum > 0:
            mean = sum(w * m for w, m in diffs) / wsum
            volatility = (sum(w * (m - mean) ** 2 for w, m in diffs) / wsum) ** 0.5

    projection = _project(cand_list, seats, reported_weight, current_total, volatility,
                          n_reported=reported_n, n_total=total)

    out = {'exists': True, 'office': office_name, 'party': party, 'county': county,
           'district': district, 'seats': seats, 'candidates': cand_list,
           'reporting': {'reported': reported_n, 'total': total,
                         'pct': round(100 * reported_n / total) if total else 0,
                         'expected_in': round(100 * reported_weight),
                         'projected_total': round(projected_total)},
           'projection': projection}
    if with_precincts:
        out['precincts'] = precincts
        out['towns'] = sorted({re.sub(r'\s+Ward\s+\d+\*?$', '', p['name']).strip() for p in precincts})
    return out


@app.route('/api/precinct-geo-map')
def api_precinct_geo_map():
    """municipality -> base house district code (e.g. 'HI21'), for single-municipality
    districts only. Lets the client draw ward-level precinct polygons: each city ward
    is its own single-municipality base house district, so its polygon is that
    district's shape in nh-house-districts.geojson. Towns not here fall back to
    nh-towns.geojson by name."""
    conn = _contested_db()
    cur = conn.cursor()
    oid = cur.execute("SELECT id FROM offices WHERE name='State Representative'").fetchone()['id']
    rows = cur.execute("""SELECT county, district, GROUP_CONCAT(municipality, '|') AS m
                          FROM municipality_districts WHERE office_id = ? AND county != ''
                          GROUP BY county, district""", (oid,)).fetchall()
    out = {}
    for r in rows:
        muns = r['m'].split('|')
        if len(muns) == 1:
            out[muns[0]] = _district_code('house', r['county'], r['district'])
    conn.close()
    return jsonify(out)


@app.route('/api/contested/<office_key>/results')
def api_contested_results(office_key):
    """Overall + precinct-by-precinct results + projection for one district."""
    if office_key not in CONTESTED_OFFICES:
        return jsonify({'error': 'unknown office'}), 404
    office_name, level = CONTESTED_OFFICES[office_key]
    party = request.args.get('party', 'R')
    party_full = 'Republican' if party == 'R' else 'Democratic'
    county, district = _decode_code(level, request.args.get('code', ''))
    demo = request.args.get('demo') == '1'
    conn = _contested_db()
    out = _compute_results(conn.cursor(), office_name, level, county, district, party_full, party, demo=demo)
    conn.close()
    return jsonify(out)


@app.route('/api/contested/<office_key>/board')
def api_contested_board(office_key):
    """Results board: every contested district for a party, with current leader,
    % of expected vote counted, and called / too-close / awaiting status. The
    results-first, auto-refreshable view of election night."""
    if office_key not in CONTESTED_OFFICES:
        return jsonify({'error': 'unknown office'}), 404
    office_name, level = CONTESTED_OFFICES[office_key]
    party = request.args.get('party', 'R')
    party_full = 'Republican' if party == 'R' else 'Democratic'
    demo = request.args.get('demo') == '1'

    conn = _contested_db()
    cur = conn.cursor()
    # Contested races for this party+office.
    rows = cur.execute("""
        SELECT r.county AS county, r.district AS district, r.seats AS seats,
               COUNT(rc.candidate_id) AS ncand
        FROM races r
        JOIN elections e ON r.election_id = e.id
        JOIN offices o   ON r.office_id = o.id
        JOIN race_candidates rc ON rc.race_id = r.id AND rc.recruitment_filing_id > 0
        WHERE e.year = 2026 AND e.election_type = 'state_primary' AND e.party = ? AND o.name = ?
        GROUP BY r.id HAVING ncand > r.seats
    """, (party_full, office_name)).fetchall()

    board = []
    for row in rows:
        county, district = row['county'] or '', row['district'] or ''
        r = _compute_results(cur, office_name, level, county, district, party_full, party, with_precincts=False, demo=demo)
        r['code'] = _district_code(level, county, district)
        board.append(r)
    conn.close()
    # Sort: in-progress/too-close first, then awaiting, then clinched; by expected_in desc.
    order = {'too_close': 0, 'leaning': 1, 'likely': 2, 'called': 3, 'awaiting': 4}
    board.sort(key=lambda x: (order.get(x['projection']['status'], 3),
                              -(x['reporting']['expected_in'])))
    return jsonify({'office': office_name, 'party': party, 'level': level, 'races': board})


@app.route('/api/export/<data_type>')
def api_export(data_type):
    """Export data as CSV or JSON."""
    format_ = request.args.get('format', 'json')
    year = request.args.get('year', type=int)

    if data_type == 'towns':
        data = analysis.export_town_data(year)
    elif data_type == 'districts':
        data = analysis.export_district_data(year)
    elif data_type == 'races':
        data = analysis.export_race_data(year)
    elif data_type == 'candidates':
        data = analysis.export_candidate_data(year)
    else:
        return jsonify({'error': 'Invalid data type'}), 400

    if format_ == 'csv':
        import csv
        import io
        output = io.StringIO()
        if data:
            writer = csv.DictWriter(output, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        response = app.response_class(
            output.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={data_type}.csv'}
        )
        return response

    return jsonify(data)


@app.route('/api/export/race-towns')
def api_export_race_towns():
    """Get per-town results for a specific office and year.

    Used by mobile app to show town-by-town breakdowns for statewide races.
    Returns candidates with votes per municipality, plus county aggregations.
    """
    office = request.args.get('office', '')
    year = request.args.get('year', type=int)
    county = request.args.get('county', '')
    district = request.args.get('district', '')

    if not office or not year:
        return jsonify({'error': 'office and year parameters required'}), 400

    conn = queries.get_connection()
    cursor = conn.cursor()

    # Build query for per-town results
    query = """
        SELECT
            res.municipality,
            c.name as candidate,
            c.party,
            SUM(res.votes) as votes
        FROM results res
        JOIN candidates c ON res.candidate_id = c.id
        JOIN races r ON res.race_id = r.id
        JOIN elections e ON r.election_id = e.id
        JOIN offices o ON r.office_id = o.id
        WHERE o.name = ?
        AND e.year = ?
        AND e.election_type = 'general'
        AND c.name NOT IN ('Undervotes', 'Overvotes', 'Write-Ins')
    """
    params = [office, year]

    if county:
        query += " AND r.county = ?"
        params.append(county)
    if district:
        query += " AND r.district = ?"
        params.append(district)

    query += " GROUP BY res.municipality, c.name, c.party ORDER BY res.municipality, votes DESC"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    # Group by municipality
    towns = {}
    candidates_set = {}  # Track unique candidates with party
    for row in rows:
        municipality = row['municipality']
        candidate = row['candidate']
        party = row['party']
        votes = row['votes']

        if municipality not in towns:
            towns[municipality] = {}
        towns[municipality][candidate] = votes

        if candidate not in candidates_set:
            candidates_set[candidate] = party

    # Build candidate list sorted by total votes
    candidate_totals = {}
    for town_data in towns.values():
        for cand, votes in town_data.items():
            candidate_totals[cand] = candidate_totals.get(cand, 0) + votes

    candidates = sorted(candidate_totals.keys(), key=lambda c: candidate_totals[c], reverse=True)

    # Build town results list
    town_results = []
    for municipality, cand_votes in towns.items():
        entry = {'town': municipality, 'votes': {}}
        for cand in candidates:
            entry['votes'][cand] = cand_votes.get(cand, 0)
        # Calculate R vs D margin
        r_votes = sum(v for c, v in cand_votes.items() if candidates_set.get(c) == 'Republican')
        d_votes = sum(v for c, v in cand_votes.items() if candidates_set.get(c) == 'Democratic')
        total = r_votes + d_votes
        entry['r_votes'] = r_votes
        entry['d_votes'] = d_votes
        entry['total_votes'] = sum(cand_votes.values())
        entry['margin'] = float(round((r_votes - d_votes) / total * 100, 1)) if total > 0 else 0.0
        town_results.append(entry)

    # Sort by total votes descending
    town_results.sort(key=lambda x: x['total_votes'], reverse=True)

    # Build candidate info list
    candidate_info = []
    for cand in candidates:
        candidate_info.append({
            'name': cand,
            'party': candidates_set.get(cand, ''),
            'total_votes': candidate_totals[cand]
        })

    return jsonify({
        'office': office,
        'year': year,
        'candidates': candidate_info,
        'towns': town_results,
        'total_towns': len(town_results)
    })


@app.route('/deep-analysis')
def deep_analysis():
    """Deep analysis page with undervotes, turnout, ticket splitting, bellwethers."""
    undervote = analysis.get_undervote_analysis()
    turnout = analysis.get_turnout_patterns()
    splitting = analysis.get_ticket_splitting_analysis()
    bellwether = analysis.get_bellwether_analysis()

    return render_template('deep_analysis.html',
                         undervote=undervote,
                         turnout=turnout,
                         splitting=splitting,
                         bellwether=bellwether)


@app.route('/stats')
def stats():
    """Comprehensive statistical analysis page."""
    swing = analysis.get_swing_analysis()
    multi_seat = analysis.get_multi_seat_analysis()
    correlation = analysis.get_correlation_analysis()
    trends = analysis.get_long_term_trends()

    return render_template('stats.html',
                         swing=swing,
                         multi_seat=multi_seat,
                         correlation=correlation,
                         trends=trends)


# ============== LIVE RESULTS ==============

def get_registered_voters_count(towns):
    """Get count of registered voters in given towns from voter API."""
    import requests
    try:
        # Query voter API for each town and sum
        api_url = "http://138.197.36.143:5050"
        api_key = os.environ.get('VOTER_API_KEY', '')

        if not api_key:
            return None

        total = 0
        for town in towns:
            resp = requests.get(
                f"{api_url}/api/count",
                params={'city': town},
                headers={'X-API-Key': api_key},
                timeout=5
            )
            if resp.ok:
                data = resp.json()
                total += data.get('count', 0)
        return total
    except Exception as e:
        print(f"Error getting voter count: {e}")
        return None


@app.route('/live/<int:election_id>')
def live_results(election_id):
    """Live results display for an election (e.g., special primary)."""
    import sqlite3
    conn = sqlite3.connect('nh_elections.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get election info
    cursor.execute("SELECT * FROM elections WHERE id = ?", (election_id,))
    election = cursor.fetchone()
    if not election:
        return "Election not found", 404

    # Get all races in this election
    cursor.execute("""
        SELECT r.*, o.name as office_name, COALESCE(r.is_official, 0) as is_official
        FROM races r
        JOIN offices o ON r.office_id = o.id
        WHERE r.election_id = ?
        ORDER BY o.name, r.county, r.district
    """, (election_id,))
    races = [dict(row) for row in cursor.fetchall()]

    # For each race, get candidates, results, and calculate stats
    race_data = []
    for race in races:
        race_id = race['id']

        # Get candidates with total votes (include zero-vote candidates)
        cursor.execute("""
            SELECT c.id, c.name, c.party, COALESCE(SUM(res.votes), 0) as total_votes
            FROM candidates c
            JOIN results res ON c.id = res.candidate_id AND res.race_id = ?
            GROUP BY c.id
            ORDER BY total_votes DESC
        """, (race_id,))
        candidates = [dict(row) for row in cursor.fetchall()]

        # Calculate total votes in race
        total_votes = sum(c['total_votes'] for c in candidates)

        # Add percentage to each candidate
        is_primary = election['election_type'] in ('special_primary', 'state_primary')
        if is_primary:
            # For primaries, calculate within party
            party_totals = {}
            for c in candidates:
                party = c['party']
                if party not in party_totals:
                    party_totals[party] = 0
                party_totals[party] += c['total_votes']
            for c in candidates:
                party_total = party_totals.get(c['party'], 0)
                c['percentage'] = round(c['total_votes'] / party_total * 100, 1) if party_total > 0 else 0
        else:
            # For general elections, calculate against total votes
            for c in candidates:
                c['percentage'] = round(c['total_votes'] / total_votes * 100, 1) if total_votes > 0 else 0

        # Get town-level results for the map
        # For primaries, we color by leading candidate, not party
        cursor.execute("""
            SELECT res.municipality, c.id as candidate_id, c.name, c.party, res.votes
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            WHERE res.race_id = ?
            ORDER BY res.municipality, res.votes DESC
        """, (race_id,))

        town_results = {}
        current_town = None
        town_candidates_temp = []

        for row in cursor.fetchall():
            town = row['municipality']
            if town != current_town:
                if current_town and town_candidates_temp:
                    # Process previous town
                    total = sum(tc['votes'] for tc in town_candidates_temp)
                    leader = town_candidates_temp[0] if town_candidates_temp else None
                    second = town_candidates_temp[1] if len(town_candidates_temp) > 1 else None
                    margin = 0
                    if total > 0 and leader and second:
                        margin = round((leader['votes'] - second['votes']) / total * 100, 1)
                    town_results[current_town] = {
                        'total': total,
                        'leader': leader['name'] if leader else None,
                        'leader_party': leader['party'] if leader else None,
                        'leader_votes': leader['votes'] if leader else 0,
                        'margin': margin,
                        'reported': total > 0
                    }
                current_town = town
                town_candidates_temp = []

            town_candidates_temp.append({
                'id': row['candidate_id'],
                'name': row['name'],
                'party': row['party'],
                'votes': row['votes']
            })

        # Process last town
        if current_town and town_candidates_temp:
            total = sum(tc['votes'] for tc in town_candidates_temp)
            leader = town_candidates_temp[0] if town_candidates_temp else None
            second = town_candidates_temp[1] if len(town_candidates_temp) > 1 else None
            margin = 0
            if total > 0 and leader and second:
                margin = round((leader['votes'] - second['votes']) / total * 100, 1)
            town_results[current_town] = {
                'total': total,
                'leader': leader['name'] if leader else None,
                'leader_party': leader['party'] if leader else None,
                'leader_votes': leader['votes'] if leader else 0,
                'margin': margin,
                'reported': total > 0
            }

        # Get individual candidate results by town for hover
        cursor.execute("""
            SELECT res.municipality, c.id as candidate_id, c.name, c.party, res.votes
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            WHERE res.race_id = ?
            ORDER BY res.municipality, res.votes DESC
        """, (race_id,))
        town_candidate_results = {}
        for row in cursor.fetchall():
            town = row['municipality']
            if town not in town_candidate_results:
                town_candidate_results[town] = []
            town_candidate_results[town].append({
                'name': row['name'],
                'party': row['party'],
                'votes': row['votes']
            })

        # Get 2024 turnout for weighted percentage calculation
        cursor.execute("""
            SELECT res.municipality, SUM(res.votes) as votes_2024
            FROM results res
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            WHERE e.year = 2024 AND e.election_type = 'general'
            AND r.county = ? AND r.district = ?
            GROUP BY res.municipality
        """, (race['county'], race['district']))
        turnout_2024 = {row['municipality']: row['votes_2024'] for row in cursor.fetchall()}

        # Calculate weighted percentage reported
        total_expected = sum(turnout_2024.values()) if turnout_2024 else 0
        reported_weight = 0
        towns_reporting = 0
        towns_total = len(turnout_2024)

        for town, data in town_results.items():
            if data['reported'] and town in turnout_2024:
                reported_weight += turnout_2024[town]
                towns_reporting += 1

        pct_reported = round(reported_weight / total_expected * 100, 1) if total_expected > 0 else 0

        # Calculate projected winner and win probability
        leader = candidates[0] if candidates else None
        projected_total = 0
        projected_votes = {c['id']: c['total_votes'] for c in candidates}

        if leader and pct_reported > 0:
            # Project remaining votes based on current ratios
            remaining_pct = 100 - pct_reported
            for c in candidates:
                ratio = c['total_votes'] / total_votes if total_votes > 0 else 0
                projected_votes[c['id']] = c['total_votes'] + (ratio * total_votes * remaining_pct / pct_reported) if pct_reported > 0 else c['total_votes']

            projected_total = sum(projected_votes.values())

        # Add projection to candidates
        for c in candidates:
            c['projected_votes'] = round(projected_votes.get(c['id'], c['total_votes']))
            c['projected_pct'] = round(c['projected_votes'] / projected_total * 100, 1) if projected_total > 0 else c['percentage']

        # Win probability (simple model based on lead and reporting)
        win_probability = None
        if len(candidates) >= 2 and pct_reported > 10:
            lead = candidates[0]['total_votes'] - candidates[1]['total_votes']
            margin_pct = (candidates[0]['percentage'] - candidates[1]['percentage'])
            # Simple model: higher lead + more reported = higher confidence
            confidence = min(99, max(1, 50 + margin_pct * 2 + pct_reported * 0.3))
            win_probability = round(confidence)

        # Get historical results for this district
        historical = {}
        cursor.execute("""
            SELECT e.year, c.name, c.party, SUM(res.votes) as votes,
                   r.seats,
                   ROW_NUMBER() OVER (PARTITION BY e.year ORDER BY SUM(res.votes) DESC) as rank
            FROM results res
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN candidates c ON res.candidate_id = c.id
            JOIN offices o ON r.office_id = o.id
            WHERE r.county = ? AND r.district = ? AND o.name = ?
            AND e.election_type = 'general'
            AND e.year < 2026
            AND e.redistricting_cycle = (SELECT redistricting_cycle FROM elections WHERE id = ?)
            GROUP BY e.year, c.id
            ORDER BY e.year DESC, votes DESC
        """, (race['county'], race['district'], race['office_name'], election['id']))

        for row in cursor.fetchall():
            year = row['year']
            if year not in historical:
                historical[year] = {'results': [], 'turnout': 0, 'seats': row['seats']}
            historical[year]['results'].append({
                'name': row['name'],
                'party': row['party'],
                'votes': row['votes'],
                'is_winner': row['rank'] <= row['seats']
            })
            historical[year]['turnout'] += row['votes']

        # Keep only last 3 elections
        historical = dict(list(historical.items())[:3])

        # Get per-town historical results
        historical_by_town = {}
        cursor.execute("""
            SELECT e.year, res.municipality, c.name, c.party, res.votes
            FROM results res
            JOIN races r ON res.race_id = r.id
            JOIN elections e ON r.election_id = e.id
            JOIN candidates c ON res.candidate_id = c.id
            JOIN offices o ON r.office_id = o.id
            WHERE r.county = ? AND r.district = ? AND o.name = ?
            AND e.election_type = 'general'
            AND e.year < 2026
            AND e.redistricting_cycle = (SELECT redistricting_cycle FROM elections WHERE id = ?)
            AND c.party IN ('Republican', 'Democratic')
            ORDER BY e.year DESC, res.municipality, res.votes DESC
        """, (race['county'], race['district'], race['office_name'], election['id']))
        for row in cursor.fetchall():
            year = row['year']
            if year not in historical:
                continue
            town = row['municipality']
            if year not in historical_by_town:
                historical_by_town[year] = {}
            if town not in historical_by_town[year]:
                historical_by_town[year][town] = []
            historical_by_town[year][town].append({
                'name': row['name'],
                'party': row['party'],
                'votes': row['votes']
            })

        # Get registered voter count for turnout calculation
        registered_voters = get_registered_voters_count(list(turnout_2024.keys()))
        turnout_pct = round(total_votes / registered_voters * 100, 1) if registered_voters and registered_voters > 0 else None

        race_data.append({
            'race': race,
            'candidates': candidates,
            'total_votes': total_votes,
            'town_results': town_results,
            'town_candidate_results': town_candidate_results,
            'pct_reported': pct_reported,
            'towns_reporting': towns_reporting,
            'towns_total': towns_total,
            'win_probability': win_probability,
            'towns': list(turnout_2024.keys()),
            'historical': historical,
            'historical_by_town': historical_by_town,
            'registered_voters': registered_voters,
            'turnout_pct': turnout_pct
        })

    conn.close()

    return render_template('live_results.html',
                         election=dict(election),
                         race_data=race_data)


@app.route('/api/live/<int:election_id>')
def api_live_results(election_id):
    """API endpoint for live results polling."""
    import sqlite3
    conn = sqlite3.connect('nh_elections.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM elections WHERE id = ?", (election_id,))
    election = cursor.fetchone()
    if not election:
        return jsonify({'error': 'Election not found'}), 404

    cursor.execute("""
        SELECT r.id, r.county, r.district, o.name as office_name,
               (SELECT SUM(votes) FROM results WHERE race_id = r.id) as total_votes
        FROM races r
        JOIN offices o ON r.office_id = o.id
        WHERE r.election_id = ?
    """, (election_id,))

    races = []
    for race_row in cursor.fetchall():
        race_id = race_row['id']

        cursor.execute("""
            SELECT c.id, c.name, c.party, COALESCE(SUM(res.votes), 0) as votes
            FROM candidates c
            JOIN results res ON c.id = res.candidate_id AND res.race_id = ?
            GROUP BY c.id
            ORDER BY votes DESC
        """, (race_id,))

        candidates = [dict(row) for row in cursor.fetchall()]
        total = sum(c['votes'] for c in candidates)

        for c in candidates:
            c['percentage'] = round(c['votes'] / total * 100, 1) if total > 0 else 0

        # Town results
        cursor.execute("""
            SELECT res.municipality, c.name, c.party, res.votes
            FROM results res
            JOIN candidates c ON res.candidate_id = c.id
            WHERE res.race_id = ?
        """, (race_id,))

        town_data = {}
        for row in cursor.fetchall():
            town = row['municipality']
            if town not in town_data:
                town_data[town] = {'candidates': [], 'total': 0}
            town_data[town]['candidates'].append({
                'name': row['name'],
                'party': row['party'],
                'votes': row['votes']
            })
            town_data[town]['total'] += row['votes']

        races.append({
            'id': race_id,
            'office': race_row['office_name'],
            'county': race_row['county'],
            'district': race_row['district'],
            'candidates': candidates,
            'total_votes': total,
            'towns': town_data
        })

    conn.close()
    return jsonify({'election': dict(election), 'races': races})


if __name__ == '__main__':
    app.run(debug=True, port=5001)
