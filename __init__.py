from flask import Flask, render_template, redirect, url_for, request
from tables import Results
import sparql_queries
from IPython.display import HTML
import ssl
from pretty_html_table import build_table

app = Flask(__name__)
#app.secret_key = "super secret key"

#context = ssl.create_default_context()
@app.route("/")
def index():
    return render_template('index.html')


@app.route('/query/<input>')
def query(input):
    airquality_results = sparql_queries.get_airquality(input)
    airquality_plots = sparql_queries.build_airquality_graphs(airquality_results)
    # facility_results = sparql_queries.get_environmental_facilities(input)
    openei_organization_results = sparql_queries.run_clean_energy_company_by_country(input)
    openei_tools_by_country_results = sparql_queries.run_tools_by_country(input)
    openei_policy_by_country = sparql_queries.run_policy_by_country(input)

    return render_template('results.html', graphs=airquality_plots,
                           tables=[HTML(build_table(airquality_results, 'blue_dark')),
                                   # HTML(build_table(facility_results, 'blue_dark')),
                                   HTML(build_table(openei_organization_results, 'blue_dark')),
                                   HTML(build_table(openei_tools_by_country_results, 'blue_dark')),
                                   HTML(build_table(openei_policy_by_country, 'blue_dark'))],
                           # TRY HTML(build_table(openei_policy_by_country, 'blue_dark').to_html(escape=False))],
                           # HTML(pandas_.to_html(escape=False))
                           titles=["Airquality results in " + input,
                                   # facility_results.columns.values,
                                   # openei_organization_results.columns.values,
                                   "Tools by Country results in " + input,
                                   "Policy by Country results in " + input])


@app.route('/sparql_query', methods=("POST", "GET"))
def sparql_query():
    if request.method == 'POST':
        input = request.form['city']
        return redirect(url_for('query', input=input))
    else:
        return render_template('index.html')


if __name__ == '__main__':
    app.debug = True
    app.run()
