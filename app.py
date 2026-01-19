#!/usr/bin/env python3
"""
NH Election Results Explorer
Insight-driven web app for exploring NH election data
"""

import os
from flask import Flask, render_template, jsonify, request
import queries
import analysis
import census

app = Flask(__name__)
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
    lean = analysis.classify_lean(pvi['current_pvi'])

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

    return render_template('district.html',
                         info=info,
                         by_year=by_year,
                         pvi=pvi,
                         lean=lean,
                         topline=topline,
                         demographics=demographics)


@app.route('/county/<name>')
def county(name):
    """County overview page."""
    summary = analysis.get_county_summary(name)
    if not summary:
        return f"County '{name}' not found", 404

    return render_template('county.html', summary=summary)


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

    return render_template('statewide_district.html',
                         info=info,
                         by_year=by_year,
                         pvi=pvi,
                         lean=lean,
                         topline=topline,
                         demographics=demographics)


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


@app.route('/office/<office_name>')
def office_detail(office_name):
    """Office-level results page."""
    # Decode URL-safe office name
    office_map = {
        'president': 'President of the United States',
        'governor': 'Governor',
        'us-senate': 'United States Senator',
        'us-house': 'Representative in Congress',
        'state-senate': 'State Senator',
        'state-house': 'State Representative',
        'exec-council': 'Executive Councilor'
    }
    office = office_map.get(office_name)
    if not office:
        return f"Office '{office_name}' not found", 404

    office_data = analysis.get_office_results(office)
    return render_template('office.html', office=office, office_name=office_name, data=office_data)


@app.route('/office/<office_name>/<int:year>')
def office_year(office_name, year):
    """Office results for a specific year with all races."""
    office_map = {
        'president': 'President of the United States',
        'governor': 'Governor',
        'us-senate': 'United States Senator',
        'us-house': 'Representative in Congress',
        'state-senate': 'State Senator',
        'state-house': 'State Representative',
        'exec-council': 'Executive Councilor'
    }
    office = office_map.get(office_name)
    if not office:
        return f"Office '{office_name}' not found", 404

    races = analysis.get_office_year_results(office, year)
    if not races:
        return f"No results for {office} in {year}", 404

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

    return render_template('office_year.html',
                         office=office,
                         office_name=office_name,
                         year=year,
                         races=races,
                         by_county=by_county,
                         total_r_seats=total_r_seats,
                         total_d_seats=total_d_seats,
                         total_r_votes=total_r_votes,
                         total_d_votes=total_d_votes)


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


if __name__ == '__main__':
    app.run(debug=True, port=5001)
