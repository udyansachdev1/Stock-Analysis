import pandas as pd
import matplotlib.pyplot as plt

# Load the test results into a pandas DataFrame
df = pd.read_csv("results\loadtest_stats_history.csv")

# Calculate the average latency per request for each level of load
average_latencies = df.groupby("User Count")["Total Median Response Time"].mean()

# Plot the average latency per request against the number of requests per second
plt.plot(average_latencies.index, average_latencies.values)
plt.xlabel("Users")
plt.ylabel("Median Response Time (ms)")
plt.title("System Performance")

plt.savefig("results/system_performance.png")

plt.show()
