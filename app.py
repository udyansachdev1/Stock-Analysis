import os
from flask import (
    Flask,
    jsonify,
    render_template,
    request,
    url_for,
    redirect,
    session,  # noqa F401
)
import pandas as pd
import plotly.graph_objects as go
import base64
from flask_sqlalchemy import SQLAlchemy
from flask_mail import Mail, Message
from sqlalchemy import text
import http.client
import urllib.parse
import json
import finnhub
from datetime import datetime
import pytz
import numpy as np
from databricks import sql

app = Flask(__name__)

## connection to fetch data from azure database
connection = sql.connect(
    server_hostname="adb-1767346969741129.9.azuredatabricks.net",
    http_path="/sql/1.0/warehouses/a20e8d116459b6d7",
    access_token="dapib2db61506df9e84e6e932b33e8600dd4-3",
)

# Configure SQLAlchemy for database
app.config[
    "SQLALCHEMY_DATABASE_URI"
] = "sqlite:///predictions.db"  # SQLite database file path
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)

app.config["MAIL_SERVER"] = "smtp.gmail.com"
app.config["MAIL_PORT"] = 465
app.config["MAIL_USERNAME"] = "udyansachdev2@gmail.com"
app.config["MAIL_PASSWORD"] = "hmoe jevw caji ewrc"
# app.config['MAIL_USE_TLS'] = True
app.config["MAIL_USE_SSL"] = True

mail = Mail(app)
with app.app_context():
    db.session.execute(text("DROP TABLE IF EXISTS prediction"))
    db.session.commit()

# Recreate the Prediction table based on the updated model
with app.app_context():
    db.create_all()


# Define the Prediction model for SQLAlchemy
class Prediction(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    ticker = db.Column(db.String(10))
    name = db.Column(db.String(100))
    recent_date = db.Column(db.String(20))
    last_close_price = db.Column(db.Float)  # Adding last_close_price column
    open_price = db.Column(db.Float)
    high_price = db.Column(db.Float)
    low_price = db.Column(db.Float)
    country = db.Column(db.String(100))
    sector = db.Column(db.String(100))
    industry = db.Column(db.String(100))
    ten_day_ma = db.Column(db.Float)


# Create tables in the database (run this once to create the tables)
with app.app_context():
    db.create_all()


def generate_time_series_plot(data):
    fig = go.Figure(
        data=[
            go.Candlestick(
                x=data["timestamp"],
                open=data["open"],
                high=data["high"],
                low=data["low"],
                close=data["close"],
            )
        ]
    )

    fig.update_layout(
        title="Stock Candlestick Chart",
        xaxis_title="Date",
        yaxis_title="Price",
        showlegend=False,
    )

    # Convert the plot to a div string using Plotly's to_html method
    plot_div = fig.to_html(full_html=False, include_plotlyjs="cdn")

    # Encode the plot div string to base64
    encoded_plot = base64.b64encode(plot_div.encode()).decode("utf-8")

    return encoded_plot


@app.route("/")
def index():
    return render_template("stock_prediction.html")


@app.route("/predict", methods=["POST"])
def predict():
    try:
        ticker = request.form["ticker"]
        rows = 1000
        # Retrieve historical data
        # stock_data = pd.read_csv("Final_Merge.csv")
        connection = sql.connect(
            server_hostname="adb-1767346969741129.9.azuredatabricks.net",
            http_path="/sql/1.0/warehouses/a20e8d116459b6d7",
            access_token="dapib2db61506df9e84e6e932b33e8600dd4-3",
        )

        cursor = connection.cursor()
        query = (
            "SELECT * FROM historical_data where Ticker = '"
            + ticker
            + "' order by timestamp desc limit "
            + str(rows)
        )
        cursor.execute(query)
        stock_data = pd.DataFrame(cursor.fetchall())
        stock_data.columns = [desc[0] for desc in cursor.description]
        stock_data["timestamp"] = pd.to_datetime(stock_data["timestamp"])
        stock_data["timestamp"] = stock_data["timestamp"].dt.date

        cursor.close()
        connection.close()

        finnhub_client = finnhub.Client(
            api_key="clla0nhr01qhqdq2t4e0clla0nhr01qhqdq2t4eg"
        )

        res = finnhub_client.quote(ticker)
        appended_data = pd.DataFrame(res, index=[0])
        appended_data["Symbol"] = ticker

        appended_data.columns = [
            "current/close",
            "change",
            "percent_change",
            "high",
            "low",
            "open",
            "previous_close",
            "timestamp",
            "Symbol",
        ]

        # Function to convert epoch timestamp to date in EST
        def convert_timestamp_to_date(timestamp):
            # Convert epoch to datetime object
            date = datetime.fromtimestamp(timestamp)

            # Define the EST timezone
            est = pytz.timezone("US/Eastern")

            # Localize to UTC, then convert to EST
            date = pytz.utc.localize(date).astimezone(est)

            # Format the date
            return date.strftime("%Y-%m-%d")

        # Apply the function to the DataFrame column
        appended_data["timestamp"] = appended_data["timestamp"].apply(
            convert_timestamp_to_date
        )

        # Align columns by adding missing columns with NaN values
        missing_cols_df1 = appended_data.columns.difference(stock_data.columns)
        missing_cols_df2 = stock_data.columns.difference(appended_data.columns)

        for col in missing_cols_df1:
            stock_data[col] = np.nan

        for col in missing_cols_df2:
            appended_data[col] = np.nan

        # Concatenate the DataFrames
        result = pd.concat([stock_data, appended_data], ignore_index=True)

        result = result.sort_values(
            by=["Symbol", "timestamp"], ascending=False
        ).reset_index(drop=True)

        ticker_data = pd.read_csv("data/Tickers_Full.csv")
        result = pd.merge(result, ticker_data, on="Ticker", how="left")
        result["close"] = result["close"].fillna(result.pop("current/close"))
        result["Name"] = result["Name"].bfill()
        result["Country"] = result["Country"].bfill()
        result["Ticker"] = result["Ticker"].bfill()
        result["Sector"] = result["Sector"].bfill()
        result["Industry"] = result["Industry"].bfill()

        # result = result[result["Symbol"] == ticker]

        result["10_day_MA"] = result["close"].rolling(window=10).mean().round(3)
        result["10_day_MA"] = result["10_day_MA"].bfill()

        # Get the last row of the data (latest date)
        last_row = result.iloc[0]

        prediction = {
            "Name": last_row["Name"],
            "Symbol": ticker,
            "RecentDate": last_row["timestamp"],
            "Open": last_row["open"],
            "High": last_row["high"],
            "Low": last_row["low"],
            "Last Close Price": last_row["close"],
            "Previous Close": last_row["previous_close"],
            "Country": last_row["Country"],
            "Sector": last_row["Sector"],
            "Industry": last_row["Industry"],
            "10_day_MA": last_row["10_day_MA"],  # Adding 10-day MA to prediction
        }

        # Generate time series plot
        plot_url = generate_time_series_plot(result)

        # Fetch news data for the selected ticker
        conn = http.client.HTTPSConnection("api.marketaux.com")
        params = urllib.parse.urlencode(
            {
                "api_token": "2YOdCqufLnYAbWCgPU0cZajCbMn8wtkU7Jw4KOmb",
                "symbols": ticker,  # Fetch news data for selected ticker
                "limit": 10,
            }
        )
        conn.request("GET", "/v1/news/all?{}".format(params))
        res = conn.getresponse()
        news_data = res.read()

        json_format = json.loads(news_data.decode("utf-8"))

        final = []
        for i in range(0, 3):
            entities = json_format["data"][i]
            df = pd.DataFrame(entities.items())
            df.columns = ["metric", "values"]
            final.append(df)

        # Merge DataFrames based on the 'symbol' column (assuming it's the common column)
        final_out = final[0]  # Start with the first DataFrame
        for df in final[1:]:
            final_out = final_out.merge(df, on="metric", how="left")

        selected_metrics = ["title", "description", "snippet", "url"]
        extracted_data = final_out[final_out["metric"].isin(selected_metrics)]
        del extracted_data[extracted_data.columns[0]]
        extracted_data.columns = ["News Article 1", "News Article 2", "News Article 3"]
        extracted_data_dict = {
            col: extracted_data[col].tolist() for col in extracted_data.columns
        }

        # extracted_string = ""

        # for column in extracted_data.columns:
        #   extracted_string += f"{column.capitalize()}: \n"
        #  extracted_string += '\n'.join([f"{value}" for value in extracted_data[column]])
        # extracted_string += "\n\n"

        # Save prediction data to the database
        new_prediction = Prediction(
            ticker=ticker,
            name=last_row["Name"] if "Name" in last_row else None,
            recent_date=last_row["timestamp"] if "timestamp" in last_row else None,
            last_close_price=last_row["close"] if "close" in last_row else None,
            open_price=last_row["open"] if "open" in last_row else None,
            high_price=last_row["high"] if "high" in last_row else None,
            low_price=last_row["low"] if "low" in last_row else None,
            country=last_row["Country"] if "Country" in last_row else None,
            sector=last_row["Sector"] if "Sector" in last_row else None,
            industry=last_row["Industry"] if "Industry" in last_row else None,
            ten_day_ma=last_row["10_day_MA"] if "10_day_MA" in last_row else None
            # Add other attributes as needed
        )
        db.session.add(new_prediction)
        db.session.commit()

        return render_template(
            "stock_prediction.html",
            prediction=prediction,
            ticker=ticker,
            plot_url=plot_url,
            news_data=extracted_data_dict,
        )

    except Exception as e:
        print(e)  # Print the actual exception for debugging
        return jsonify({"error": str(e)}), 500


@app.route("/send_email", methods=["POST"])
def send_email():
    try:
        email = request.form["email"]

        # Retrieve the prediction data from the database (replace this with your logic)
        prediction = Prediction.query.order_by(Prediction.id.desc()).first()

        # Extract all attributes from the prediction instance dynamically
        prediction_details = ""
        for column in Prediction.__table__.columns:
            column_name = column.name
            column_value = getattr(prediction, column_name)
            prediction_details += f"{column_name.capitalize()}: {column_value}\n"

        # Your email sending logic using the retrieved prediction data
        msg = Message(
            "Stock Prediction", sender="your_email@gmail.com", recipients=[email]
        )
        msg.body = "Your stock prediction details...\n\n" + prediction_details
        mail.send(msg)  # Send the email

        # After sending the email, redirect to the success page
        return redirect(
            url_for("email_sent")
        )  # Assumes 'email_sent' is the endpoint for the success page

    except Exception as e:
        print(e)  # Print the actual exception for debugging
        return jsonify({"error": str(e)}), 500


@app.route("/email_sent")
def email_sent():
    # Render the success page template after the email is sent
    return render_template("email_sent.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
    