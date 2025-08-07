import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import numpy as np
import logging

logger = logging.getLogger(__name__)


class StormglassForecastDraw:
    def __init__(self, api):
        """
        Initializes the StormglassForecastDraw with a StormglassAPI instance.

        Args:
            api (StormglassAPI): The StormglassAPI instance to use for data retrieval.
        """
        self.api = api

    def plot_temperature_forecast(
        self,
        locations,
        start_datetime=None,
        end_datetime=None,
        high_resolution=True,
        figsize=(12, 8),
    ):
        """
        Plot temperature forecast for specified locations.

        Args:
            locations (list): List of (lat, lon) tuples.
            start_datetime (datetime, optional): Start datetime for the forecast.
            end_datetime (datetime, optional): End datetime for the forecast.
            high_resolution (bool): Whether to use high-resolution data (15-min intervals).
            figsize (tuple): Figure size (width, height).

        Returns:
            matplotlib.figure.Figure: The plotted figure.
        """
        if start_datetime is None:
            start_datetime = datetime.now().replace(minute=0, second=0, microsecond=0)

        if end_datetime is None:
            end_datetime = start_datetime + timedelta(hours=24)

        # Get forecast data
        if high_resolution:
            results = self.api.get_high_resolution_forecast(
                locations=locations,
                parameters=["airTemperature"],
                start_datetime=start_datetime,
                end_datetime=end_datetime,
            )
        else:
            results = self.api.get_standard_forecast(
                locations=locations,
                parameters=["airTemperature"],
                start_datetime=start_datetime,
                end_datetime=end_datetime,
            )

        if not results:
            logger.warning("No data received for temperature forecast")
            return None

        # Create the plot
        fig, ax = plt.subplots(figsize=figsize)

        colors = plt.cm.Set3(np.linspace(0, 1, len(locations)))

        for i, df in enumerate(results):
            if df.empty:
                continue

            lat, lon = df["latitude"].iloc[0], df["longitude"].iloc[0]
            location_label = f"({lat:.2f}, {lon:.2f})"

            if "airTemperature" in df.columns:
                ax.plot(
                    df["datetime"],
                    df["airTemperature"],
                    label=location_label,
                    color=colors[i],
                    linewidth=2,
                    marker="o",
                    markersize=4,
                )

        ax.set_xlabel("Time")
        ax.set_ylabel("Temperature (°C)")
        ax.set_title("Temperature Forecast")
        ax.legend()
        ax.grid(True, alpha=0.3)

        # Format x-axis
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

        plt.tight_layout()
        return fig

    def plot_wind_forecast(
        self,
        locations,
        start_datetime=None,
        end_datetime=None,
        high_resolution=True,
        figsize=(15, 10),
    ):
        """
        Plot wind speed and direction forecast for specified locations.

        Args:
            locations (list): List of (lat, lon) tuples.
            start_datetime (datetime, optional): Start datetime for the forecast.
            end_datetime (datetime, optional): End datetime for the forecast.
            high_resolution (bool): Whether to use high-resolution data (15-min intervals).
            figsize (tuple): Figure size (width, height).

        Returns:
            matplotlib.figure.Figure: The plotted figure.
        """
        if start_datetime is None:
            start_datetime = datetime.now().replace(minute=0, second=0, microsecond=0)

        if end_datetime is None:
            end_datetime = start_datetime + timedelta(hours=24)

        # Get forecast data
        if high_resolution:
            results = self.api.get_high_resolution_forecast(
                locations=locations,
                parameters=["windSpeed", "windDirection"],
                start_datetime=start_datetime,
                end_datetime=end_datetime,
            )
        else:
            results = self.api.get_standard_forecast(
                locations=locations,
                parameters=["windSpeed", "windDirection"],
                start_datetime=start_datetime,
                end_datetime=end_datetime,
            )

        if not results:
            logger.warning("No data received for wind forecast")
            return None

        # Create the plot
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=figsize)

        colors = plt.cm.Set3(np.linspace(0, 1, len(locations)))

        for i, df in enumerate(results):
            if df.empty:
                continue

            lat, lon = df["latitude"].iloc[0], df["longitude"].iloc[0]
            location_label = f"({lat:.2f}, {lon:.2f})"

            if "windSpeed" in df.columns:
                ax1.plot(
                    df["datetime"],
                    df["windSpeed"],
                    label=location_label,
                    color=colors[i],
                    linewidth=2,
                    marker="o",
                    markersize=4,
                )

            if "windDirection" in df.columns:
                ax2.plot(
                    df["datetime"],
                    df["windDirection"],
                    label=location_label,
                    color=colors[i],
                    linewidth=2,
                    marker="o",
                    markersize=4,
                )

        ax1.set_xlabel("Time")
        ax1.set_ylabel("Wind Speed (m/s)")
        ax1.set_title("Wind Speed Forecast")
        ax1.legend()
        ax1.grid(True, alpha=0.3)

        ax2.set_xlabel("Time")
        ax2.set_ylabel("Wind Direction (°)")
        ax2.set_title("Wind Direction Forecast")
        ax2.legend()
        ax2.grid(True, alpha=0.3)

        # Format x-axis
        for ax in [ax1, ax2]:
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
            ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

        plt.tight_layout()
        return fig

    def plot_comprehensive_forecast(
        self,
        locations,
        start_datetime=None,
        end_datetime=None,
        high_resolution=True,
        figsize=(20, 15),
    ):
        """
        Plot comprehensive weather forecast including temperature, wind, pressure, and humidity.

        Args:
            locations (list): List of (lat, lon) tuples.
            start_datetime (datetime, optional): Start datetime for the forecast.
            end_datetime (datetime, optional): End datetime for the forecast.
            high_resolution (bool): Whether to use high-resolution data (15-min intervals).
            figsize (tuple): Figure size (width, height).

        Returns:
            matplotlib.figure.Figure: The plotted figure.
        """
        if start_datetime is None:
            start_datetime = datetime.now().replace(minute=0, second=0, microsecond=0)

        if end_datetime is None:
            end_datetime = start_datetime + timedelta(hours=24)

        # Get forecast data
        parameters = [
            "airTemperature",
            "windSpeed",
            "windDirection",
            "pressure",
            "humidity",
            "precipitation",
        ]

        if high_resolution:
            results = self.api.get_high_resolution_forecast(
                locations=locations,
                parameters=parameters,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
            )
        else:
            results = self.api.get_standard_forecast(
                locations=locations,
                parameters=parameters,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
            )

        if not results:
            logger.warning("No data received for comprehensive forecast")
            return None

        # Create the plot
        fig, axes = plt.subplots(3, 2, figsize=figsize)
        axes = axes.flatten()

        colors = plt.cm.Set3(np.linspace(0, 1, len(locations)))

        for i, df in enumerate(results):
            if df.empty:
                continue

            lat, lon = df["latitude"].iloc[0], df["longitude"].iloc[0]
            location_label = f"({lat:.2f}, {lon:.2f})"

            # Temperature
            if "airTemperature" in df.columns:
                axes[0].plot(
                    df["datetime"],
                    df["airTemperature"],
                    label=location_label,
                    color=colors[i],
                    linewidth=2,
                    marker="o",
                    markersize=4,
                )

            # Wind Speed
            if "windSpeed" in df.columns:
                axes[1].plot(
                    df["datetime"],
                    df["windSpeed"],
                    label=location_label,
                    color=colors[i],
                    linewidth=2,
                    marker="o",
                    markersize=4,
                )

            # Wind Direction
            if "windDirection" in df.columns:
                axes[2].plot(
                    df["datetime"],
                    df["windDirection"],
                    label=location_label,
                    color=colors[i],
                    linewidth=2,
                    marker="o",
                    markersize=4,
                )

            # Pressure
            if "pressure" in df.columns:
                axes[3].plot(
                    df["datetime"],
                    df["pressure"],
                    label=location_label,
                    color=colors[i],
                    linewidth=2,
                    marker="o",
                    markersize=4,
                )

            # Humidity
            if "humidity" in df.columns:
                axes[4].plot(
                    df["datetime"],
                    df["humidity"],
                    label=location_label,
                    color=colors[i],
                    linewidth=2,
                    marker="o",
                    markersize=4,
                )

            # Precipitation
            if "precipitation" in df.columns:
                axes[5].plot(
                    df["datetime"],
                    df["precipitation"],
                    label=location_label,
                    color=colors[i],
                    linewidth=2,
                    marker="o",
                    markersize=4,
                )

        # Set titles and labels
        titles = [
            "Temperature (°C)",
            "Wind Speed (m/s)",
            "Wind Direction (°)",
            "Pressure (hPa)",
            "Humidity (%)",
            "Precipitation (mm)",
        ]

        for ax, title in zip(axes, titles):
            ax.set_title(title)
            ax.set_xlabel("Time")
            ax.legend()
            ax.grid(True, alpha=0.3)
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
            ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

        plt.tight_layout()
        return fig

    def plot_parameter_comparison(
        self,
        locations,
        parameters,
        start_datetime=None,
        end_datetime=None,
        high_resolution=True,
        figsize=(15, 10),
    ):
        """
        Plot comparison of specific parameters across locations.

        Args:
            locations (list): List of (lat, lon) tuples.
            parameters (list): List of parameters to plot.
            start_datetime (datetime, optional): Start datetime for the forecast.
            end_datetime (datetime, optional): End datetime for the forecast.
            high_resolution (bool): Whether to use high-resolution data (15-min intervals).
            figsize (tuple): Figure size (width, height).

        Returns:
            matplotlib.figure.Figure: The plotted figure.
        """
        if start_datetime is None:
            start_datetime = datetime.now().replace(minute=0, second=0, microsecond=0)

        if end_datetime is None:
            end_datetime = start_datetime + timedelta(hours=24)

        # Get forecast data
        if high_resolution:
            results = self.api.get_high_resolution_forecast(
                locations=locations,
                parameters=parameters,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
            )
        else:
            results = self.api.get_standard_forecast(
                locations=locations,
                parameters=parameters,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
            )

        if not results:
            logger.warning("No data received for parameter comparison")
            return None

        # Create the plot
        n_params = len(parameters)
        n_cols = min(3, n_params)
        n_rows = (n_params + n_cols - 1) // n_cols

        fig, axes = plt.subplots(n_rows, n_cols, figsize=figsize)
        if n_params == 1:
            axes = [axes]
        elif n_rows == 1:
            axes = axes
        else:
            axes = axes.flatten()

        colors = plt.cm.Set3(np.linspace(0, 1, len(locations)))

        for i, df in enumerate(results):
            if df.empty:
                continue

            lat, lon = df["latitude"].iloc[0], df["longitude"].iloc[0]
            location_label = f"({lat:.2f}, {lon:.2f})"

            for j, param in enumerate(parameters):
                if param in df.columns:
                    axes[j].plot(
                        df["datetime"],
                        df[param],
                        label=location_label,
                        color=colors[i],
                        linewidth=2,
                        marker="o",
                        markersize=4,
                    )

        # Set titles and labels
        for ax, param in zip(axes[:n_params], parameters):
            ax.set_title(param)
            ax.set_xlabel("Time")
            ax.legend()
            ax.grid(True, alpha=0.3)
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
            ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

        # Hide unused subplots
        for ax in axes[n_params:]:
            ax.set_visible(False)

        plt.tight_layout()
        return fig
