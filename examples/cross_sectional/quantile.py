"""
quantile - Cross-sectional rank + inverse CDF transform

When to use:
    Use quantile() to transform ranks to a specific distribution.
    Available drivers: "gaussian" (default), "uniform", "cauchy"

Parameters:
    x: Input DataFrame
    driver: Distribution type ("gaussian", "uniform", "cauchy")

Example output:
    Gaussian quantile transform of momentum
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import quantile, ts_delta

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate momentum
momentum = ts_delta(prices, 5)

# Gaussian quantile transform
gaussian_quantile = quantile(momentum, driver="gaussian")

print("quantile() - Cross-sectional quantile transform")
print("=" * 50)
print("\n5-day momentum:")
print(momentum.head(7))
print("\nGaussian quantile transform:")
print(gaussian_quantile.head(7))

# Uniform quantile transform
uniform_quantile = quantile(momentum, driver="uniform")
print("\nUniform quantile transform:")
print(uniform_quantile.head(7))

# Cleanup
client.close()
