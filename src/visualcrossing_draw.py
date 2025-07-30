"""
VisualCrossingForecastDraw - class for building weather data charts from Visual Crossing API.
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from datetime import timedelta


class VisualCrossingForecastDraw:
    def __init__(self, api):
        """
        Initializes the VisualCrossingForecastDraw with a VisualCrossingAPI instance.
        Args:
            api: An instance of the VisualCrossingAPI class.
        """
        self.api = api

    def plot_forecasts(
        self,
        locations,
        start_datetime=None,
        end_datetime=None,
        unit_group="metric",
        label_locations=True,
        figsize=(14, 4),
        time_format="%H:%M",
        date_format="%Y-%m-%d",
        show_grid=True,
    ):
        """
        Fetches forecasts for multiple locations and plots all received weather parameters for each on separate subplots.
        Args:
            locations (list): List of (lat, lon) tuples.
            start_datetime (str): Start datetime for the forecast.
            end_datetime (str): End datetime for the forecast.
            unit_group (str): Unit group for the API (default: "metric").
            label_locations (bool): Whether to label each line with its location.
            figsize (tuple): Figure size for each subplot.
            time_format (str): Format for time display on x-axis (default: "%H:%M").
            date_format (str): Format for date display on x-axis (default: "%Y-%m-%d").
            show_grid (bool): Whether to show grid on plots (default: True).
        """
        results = self.api.get_forecast(
            locations,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            unit_group=unit_group,
        )
        if not isinstance(results, list):
            results = [results]
        # Collect all parameter names (excluding metadata columns)
        metadata_cols = {
            "datetime",
            "latitude",
            "longitude",
            "datetimeEpoch",
        }
        all_params = set()
        for df in results:
            if not df.empty:
                all_params.update(
                    [col for col in df.columns if col not in metadata_cols]
                )
        # Only keep columns that are numeric or boolean for plotting
        safe_params = []
        skipped_params = []
        non_numeric_params = []

        for df in results:
            if not df.empty:
                for param in all_params:
                    if param in df.columns:
                        # Check dtype and sample values
                        series = df[param]

                        # First check if it's already numeric
                        if pd.api.types.is_numeric_dtype(
                            series
                        ) or pd.api.types.is_bool_dtype(series):
                            if param not in safe_params:
                                safe_params.append(param)
                        else:
                            # Try to convert to numeric if it's a string representation of numbers
                            try:
                                # Check if it's a string that can be converted to numeric
                                if series.dtype == "object":
                                    # Sample some non-null values to test conversion
                                    sample = series.dropna().head(10)
                                    if len(sample) > 0:
                                        # Try to convert sample to numeric
                                        numeric_sample = pd.to_numeric(
                                            sample, errors="coerce"
                                        )
                                        if not numeric_sample.isna().all():
                                            # If most values can be converted, consider it numeric
                                            if (
                                                numeric_sample.isna().sum()
                                                / len(numeric_sample)
                                                < 0.5
                                            ):
                                                if param not in safe_params:
                                                    safe_params.append(param)
                                                continue
                            except:
                                pass

                            # Check for lists/dicts in the first few non-null values
                            sample = series.dropna().head(5)
                            if any(isinstance(x, (list, dict)) for x in sample):
                                if param not in skipped_params:
                                    skipped_params.append(param)
                            else:
                                # If not list/dict but still not numeric, add to non-numeric list
                                if param not in non_numeric_params:
                                    non_numeric_params.append(param)

        # Combine all non-plottable parameters
        all_skipped = skipped_params + non_numeric_params

        if not safe_params:
            print("No plottable (numeric/bool) weather parameters found.")
            return
        if all_skipped:
            print(f"Skipped non-numeric/list/dict parameters: {all_skipped}")
        print("Available plottable parameters:", safe_params)
        # Plot each parameter as a separate subplot for each location
        for idx, (df, loc) in enumerate(zip(results, locations)):
            if df.empty:
                continue
            n_params = len(safe_params)
            fig, axes = plt.subplots(
                n_params, 1, figsize=(figsize[0], figsize[1] * n_params)
            )
            if n_params == 1:
                axes = [axes]

            # Define title with coordinates
            title = f"Weather forecast for coordinates {loc[0]:.4f}, {loc[1]:.4f}"
            fig.suptitle(title, fontsize=14, fontweight="bold")

            # Define time range for axis formatting
            time_range = df["datetime"].max() - df["datetime"].min()

            for i, param in enumerate(safe_params):
                if param not in df.columns:
                    continue

                ax = axes[i]
                ax.plot(
                    df["datetime"],
                    df[param],
                    label=param,
                    linewidth=1.5,
                    marker="o",
                    markersize=3,
                )
                ax.set_ylabel(param, fontsize=10, fontweight="bold")
                ax.legend(fontsize=9)

                # Grid settings
                if show_grid:
                    ax.grid(True, alpha=0.3, linestyle="--")

                # Time axis formatting - always show hours with dates
                # Determine if dates need to be shown
                if time_range > timedelta(hours=12):
                    # For ranges more than 12 hours show date and time
                    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
                    ax.xaxis.set_major_locator(mdates.HourLocator(interval=4))
                    ax.xaxis.set_minor_locator(mdates.HourLocator(interval=1))
                else:
                    # For shorter ranges show only hours
                    ax.xaxis.set_major_formatter(mdates.DateFormatter(time_format))
                    ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
                    ax.xaxis.set_minor_locator(mdates.MinuteLocator(interval=30))

                # Add hour labels near each graph
                time_range_str = f"{df['datetime'].min().strftime('%H:%M')} - {df['datetime'].max().strftime('%H:%M')}"
                ax.text(
                    0.02,
                    0.95,
                    f"Time: {time_range_str}",
                    transform=ax.transAxes,
                    fontsize=10,
                    fontweight="bold",
                    verticalalignment="top",
                    bbox=dict(
                        boxstyle="round,pad=0.3",
                        facecolor="white",
                        alpha=0.9,
                        edgecolor="gray",
                    ),
                )

                # Rotate labels for better readability
                plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")
                plt.setp(ax.xaxis.get_minorticklabels(), rotation=45, ha="right")

                # Add X-axis label for each graph
                ax.set_xlabel("Time", fontsize=10, fontweight="bold")

            # Automatic layout adjustment
            plt.tight_layout(rect=[0, 0.03, 1, 0.97])
            plt.show()



    def plot_comparison(
        self,
        locations,
        parameters,
        start_datetime=None,
        end_datetime=None,
        unit_group="metric",
        figsize=(14, 6),
        time_format="%H:%M",
        date_format="%Y-%m-%d",
        show_grid=True,
        location_names=None,
    ):
        """
        Plots comparison of weather parameters across multiple locations with proper time formatting.

        Args:
            locations (list): List of (lat, lon) tuples.
            parameters (list): List of weather parameters to compare.
            start_datetime (str): Start datetime for the forecast.
            end_datetime (str): End datetime for the forecast.
            unit_group (str): Unit group for the API (default: "metric").
            figsize (tuple): Figure size for the plot.
            time_format (str): Format for time display on x-axis (default: "%H:%M").
            date_format (str): Format for date display on x-axis (default: "%Y-%m-%d").
            show_grid (bool): Whether to show grid on plots (default: True).
            location_names (list, optional): List of names for locations. If None, uses coordinates.
        """
        # Get data for all locations
        results = self.api.get_forecast(
            locations,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            unit_group=unit_group,
        )

        if not isinstance(results, list):
            results = [results]

        # Check if there is data
        valid_results = [
            (df, loc) for df, loc in zip(results, locations) if not df.empty
        ]
        if not valid_results:
            print("No data for comparison.")
            return

        # Define location names
        if location_names is None:
            location_names = [f"{lat:.4f}, {lon:.4f}" for lat, lon in locations]

        # Build graphs for each parameter
        for param in parameters:
            # Check if parameter exists in all data
            available_data = []
            for df, loc in valid_results:
                if param in df.columns:
                    available_data.append((df, loc))

            if not available_data:
                print(f"Parameter '{param}' not found in data.")
                continue

            # Create graph for this parameter
            fig, ax = plt.subplots(figsize=figsize)

            # Define time range
            all_timestamps = []
            for df, _ in available_data:
                all_timestamps.extend(df["datetime"].tolist())

            if not all_timestamps:
                continue

            time_range = max(all_timestamps) - min(all_timestamps)

            # Build lines for each location
            colors = plt.cm.Set3(np.linspace(0, 1, len(available_data)))
            for i, (df, loc) in enumerate(available_data):
                loc_idx = locations.index(loc)
                loc_name = (
                    location_names[loc_idx]
                    if loc_idx < len(location_names)
                    else f"{loc[0]:.4f}, {loc[1]:.4f}"
                )

                ax.plot(
                    df["datetime"],
                    df[param],
                    label=loc_name,
                    linewidth=2,
                    marker="o",
                    markersize=4,
                    color=colors[i],
                    alpha=0.8,
                )

            # Graph settings
            ax.set_title(
                f"Comparison of parameter '{param}' for different locations",
                fontsize=14,
                fontweight="bold",
            )
            ax.set_ylabel(param, fontsize=12, fontweight="bold")
            ax.legend(fontsize=10, loc="best")

            # Grid settings
            if show_grid:
                ax.grid(True, alpha=0.3, linestyle="--")

            # Time axis formatting - always show hours with dates
            # Determine if dates need to be shown
            if time_range > timedelta(hours=12):
                # For ranges more than 12 hours show date and time
                ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
                ax.xaxis.set_major_locator(mdates.HourLocator(interval=4))
                ax.xaxis.set_minor_locator(mdates.HourLocator(interval=1))
            else:
                # For shorter ranges show only hours
                ax.xaxis.set_major_formatter(mdates.DateFormatter(time_format))
                ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
                ax.xaxis.set_minor_locator(mdates.MinuteLocator(interval=30))

            # Add hour labels near the graph
            time_range_str = f"{min(all_timestamps).strftime('%H:%M')} - {max(all_timestamps).strftime('%H:%M')}"
            ax.text(
                0.02,
                0.95,
                f"Time: {time_range_str}",
                transform=ax.transAxes,
                fontsize=10,
                fontweight="bold",
                verticalalignment="top",
                bbox=dict(
                    boxstyle="round,pad=0.3",
                    facecolor="white",
                    alpha=0.9,
                    edgecolor="gray",
                ),
            )

            # Rotate labels
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")
            plt.setp(ax.xaxis.get_minorticklabels(), rotation=45, ha="right")

            ax.set_xlabel("Time", fontsize=12, fontweight="bold")

            plt.tight_layout()
            plt.show()







