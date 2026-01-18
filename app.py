#!/usr/bin/env python3
"""
NH Election Results Explorer
Insight-driven web app for exploring NH election data
"""

from flask import Flask, render_template, jsonify, request
import queries
import analysis

app = Flask(__name__)


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
                         counties=counties)


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

    return render_template('town.html',
                         summary=summary,
                         races=races,
                         comparison=comparison,
                         pvi=pvi,
                         key_races=key_races,
                         representation=representation)


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

    # Group by year and calculate insights
    by_year = {}
    for r in results:
        year = r['year']
        if year not in by_year:
            by_year[year] = {'seats': r['seats'], 'candidates': [], 'r_seats': 0, 'd_seats': 0}
        # Map query keys to template keys
        candidate = {
            'name': r['candidate'],
            'party': r['party'],
            'votes': r['total_votes'],
            'is_winner': r['is_winner']
        }
        by_year[year]['candidates'].append(candidate)
        if r['is_winner']:
            if r['party'] == 'Republican':
                by_year[year]['r_seats'] += 1
            elif r['party'] == 'Democratic':
                by_year[year]['d_seats'] += 1

    return render_template('district.html',
                         info=info,
                         by_year=by_year)


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

    # Group by year
    by_year = {}
    for r in results:
        year = r['year']
        if year not in by_year:
            by_year[year] = {'seats': r['seats'], 'candidates': [], 'r_seats': 0, 'd_seats': 0}
        by_year[year]['candidates'].append(r)
        if r['is_winner']:
            if r['party'] == 'Republican':
                by_year[year]['r_seats'] += 1
            elif r['party'] == 'Democratic':
                by_year[year]['d_seats'] += 1

    return render_template('statewide_district.html',
                         info=info,
                         by_year=by_year)


if __name__ == '__main__':
    app.run(debug=True, port=5001)
