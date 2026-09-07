"""Automated results intake from email and Signal.

Runs as its own service (nh-results-intake.service) alongside the web app,
against the same nh_elections.db. Nothing here is imported by the Flask app
except the review blueprint in intake.review.
"""
