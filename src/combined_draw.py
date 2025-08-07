"""
CombinedForecastDraw - class for building weather data charts comparing Visual Crossing, Meteomatics, and Stormglass APIs.
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from datetime import timedelta


class CombinedForecastDraw:
    def __init__(self, visualcrossing_api, meteomatics_api, stormglass_api=None):
        """
        Initializes the CombinedForecastDraw with API instances.
        Args:
            visualcrossing_api: An instance of the VisualCrossingAPI class.
            meteomatics_api: An instance of the MeteomaticsAPI class.
            stormglass_api: An instance of the StormglassAPI class (optional).
        """
        self.visualcrossing_api = visualcrossing_api
        self.meteomatics_api = meteomatics_api
        self.stormglass_api = stormglass_api

    def _map_parameters(self, visualcrossing_parameters=None, meteomatics_parameters=None, stormglass_parameters=None):
        """
        Maps parameters between VisualCrossing, Meteomatics, and Stormglass for comparison.
        
        Args:
            visualcrossing_parameters (list, optional): VisualCrossing parameters
            meteomatics_parameters (list, optional): Meteomatics parameters
            stormglass_parameters (list, optional): Stormglass parameters
            
        Returns:
            tuple: (mapped_vc_params, mapped_mm_params, mapped_sg_params, comparison_params)
        """
        # Parameter mapping between APIs
        parameter_mapping = {
            # Temperature
            "temp": "t_2m:C",
            "feelslike": "feels_like_2m:C",
            "airTemperature": "t_2m:C",  # Stormglass to Meteomatics
            "temp": "airTemperature",  # VisualCrossing to Stormglass
            
            # Humidity
            "humidity": "rh_2m:p",
            "dew": "dew_point_2m:C",
            "humidity": "humidity",  # VisualCrossing to Stormglass
            
            # Wind
            "windspeed": "wind_speed_10m:ms",
            "winddir": "wind_dir_10m:d",
            "windSpeed": "wind_speed_10m:ms",  # Stormglass to Meteomatics
            "windDirection": "wind_dir_10m:d",  # Stormglass to Meteomatics
            "windspeed": "windSpeed",  # VisualCrossing to Stormglass
            "winddir": "windDirection",  # VisualCrossing to Stormglass
            
            # Pressure
            "pressure": "msl_pressure:hPa",
            "pressure": "pressure",  # VisualCrossing to Stormglass
            
            # Precipitation
            "precip": "precip_1h:mm",
            "precip_total": "precip_total:mm",
            "precipitation": "precip_1h:mm",  # Stormglass to Meteomatics
            "precip": "precipitation",  # VisualCrossing to Stormglass
            
            # Solar
            "solarradiation": "solar_radiation:W",
            
            # Cloud cover
            "cloudcover": "cloud_cover:p",
            "cloudCover": "cloud_cover:p",  # Stormglass to Meteomatics
            "cloudcover": "cloudCover",  # VisualCrossing to Stormglass
            
            # Visibility
            "visibility": "visibility:m",
            "visibility": "visibility",  # VisualCrossing to Stormglass
        }
        
        # Reverse mapping
        reverse_mapping = {v: k for k, v in parameter_mapping.items()}
        
        # Map VisualCrossing parameters to Meteomatics
        mapped_vc_params = visualcrossing_parameters
        if visualcrossing_parameters:
            mapped_vc_params = []
            for param in visualcrossing_parameters:
                if param in parameter_mapping:
                    mapped_vc_params.append(param)
                else:
                    # Keep unmapped parameters
                    mapped_vc_params.append(param)
        
        # Map Meteomatics parameters to VisualCrossing
        mapped_mm_params = meteomatics_parameters
        if meteomatics_parameters:
            mapped_mm_params = []
            for param in meteomatics_parameters:
                if param in reverse_mapping:
                    mapped_mm_params.append(param)
                else:
                    # Keep unmapped parameters
                    mapped_mm_params.append(param)
        
        # Map Stormglass parameters to Meteomatics
        mapped_sg_params = stormglass_parameters
        if stormglass_parameters:
            mapped_sg_params = []
            for param in stormglass_parameters:
                if param in parameter_mapping:
                    mapped_sg_params.append(param)
                else:
                    # Keep unmapped parameters
                    mapped_sg_params.append(param)
        
        # Create comparison parameters list
        comparison_params = []
        if visualcrossing_parameters:
            for param in visualcrossing_parameters:
                if param in parameter_mapping:
                    comparison_params.append(f"{param} (VC) vs {parameter_mapping[param]} (MM)")
                else:
                    comparison_params.append(f"{param} (VC only)")
        
        if meteomatics_parameters:
            for param in meteomatics_parameters:
                if param in reverse_mapping:
                    comparison_params.append(f"{reverse_mapping[param]} (VC) vs {param} (MM)")
                else:
                    comparison_params.append(f"{param} (MM only)")
        
        if stormglass_parameters:
            for param in stormglass_parameters:
                if param in parameter_mapping:
                    comparison_params.append(f"{param} (SG) vs {parameter_mapping[param]} (MM)")
                else:
                    comparison_params.append(f"{param} (SG only)")
        
        return mapped_vc_params, mapped_mm_params, mapped_sg_params, comparison_params

    def plot_comparison(
        self,
        locations,
        parameters,
        start_datetime=None,
        end_datetime=None,
        unit_group="metric",
        model="mix",
        interval=None,
        visualcrossing_parameters=None,
        meteomatics_interval=None,
        stormglass_parameters=None,
        stormglass_high_resolution=True,
        figsize=(14, 6),
        time_format="%H:%M",
        date_format="%Y-%m-%d",
        show_grid=True,
        location_names=None,
    ):
        """
        Plots comparison of weather parameters from Visual Crossing, Meteomatics, and Stormglass APIs.

        Args:
            locations (list): List of (lat, lon) tuples.
            parameters (list): List of weather parameters to compare (for Meteomatics).
            start_datetime (datetime, optional): Start datetime for the forecast.
            end_datetime (datetime, optional): End datetime for the forecast.
            unit_group (str): Unit group for Visual Crossing API (default: "metric").
            model (str, optional): Weather model for Meteomatics. Defaults to "mix".
            interval (timedelta, optional): Data interval for Meteomatics. Defaults to 1 hour.
            visualcrossing_parameters (list, optional): List of Visual Crossing parameters to compare. If None, uses all available.
            meteomatics_interval (timedelta, optional): Specific interval for Meteomatics API. Overrides interval if provided.
            stormglass_parameters (list, optional): List of Stormglass parameters to compare.
            stormglass_high_resolution (bool): Whether to use high-resolution data for Stormglass (default: True).
            figsize (tuple): Figure size for the plot.
            time_format (str): Format for time display on x-axis (default: "%H:%M").
            date_format (str): Format for date display on x-axis (default: "%Y-%m-%d").
            show_grid (bool): Whether to show grid on plots (default: True).
            location_names (list, optional): List of names for locations. If None, uses coordinates.
        """
        # Get data from all APIs
        try:
            vc_results = self.visualcrossing_api.get_forecast(
                locations,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                unit_group=unit_group,
                parameters=visualcrossing_parameters,
            )
        except Exception as e:
            print(f"Error fetching Visual Crossing data: {e}")
            vc_results = []

        try:
            # Use meteomatics_interval if provided, otherwise fall back to interval
            mm_interval = meteomatics_interval if meteomatics_interval is not None else interval
            mm_results = self.meteomatics_api.get_forecast(
                locations,
                parameters=parameters,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                model=model,
                interval=mm_interval,
            )
        except Exception as e:
            print(f"Error fetching Meteomatics data: {e}")
            mm_results = []

        # Get Stormglass data if API is available
        sg_results = []
        if self.stormglass_api and stormglass_parameters:
            try:
                if stormglass_high_resolution:
                    sg_results = self.stormglass_api.get_high_resolution_forecast(
                        locations=locations,
                        parameters=stormglass_parameters,
                        start_datetime=start_datetime,
                        end_datetime=end_datetime,
                    )
                else:
                    sg_results = self.stormglass_api.get_standard_forecast(
                        locations=locations,
                        parameters=stormglass_parameters,
                        start_datetime=start_datetime,
                        end_datetime=end_datetime,
                    )
            except Exception as e:
                print(f"Error fetching Stormglass data: {e}")
                sg_results = []

        # Check if there is data from at least one API
        valid_vc_results = [
            (df, loc, "Visual Crossing") 
            for df, loc in zip(vc_results, locations) 
            if not df.empty
        ]
        valid_mm_results = [
            (df, loc, "Meteomatics") 
            for df, loc in zip(mm_results, locations) 
            if not df.empty
        ]
        valid_sg_results = [
            (df, loc, "Stormglass") 
            for df, loc in zip(sg_results, locations) 
            if not df.empty
        ]

        if not valid_vc_results and not valid_mm_results and not valid_sg_results:
            print("No data available from any API for comparison.")
            return

        # Define location names
        if location_names is None:
            location_names = [f"{lat:.4f}, {lon:.4f}" for lat, lon in locations]

        # Map parameters between APIs
        mapped_vc_params, mapped_mm_params, mapped_sg_params, comparison_params = self._map_parameters(
            visualcrossing_parameters, parameters, stormglass_parameters
        )
        
        # Build graphs for each parameter
        all_parameters = set()
        if parameters:
            all_parameters.update(parameters)
        if stormglass_parameters:
            all_parameters.update(stormglass_parameters)
        
        for param in all_parameters:
            # Check if parameter exists in data from any API
            available_data = []
            
            # Add Visual Crossing data
            for df, loc, api_name in valid_vc_results:
                if param in df.columns:
                    available_data.append((df, loc, api_name))
            
            # Add Meteomatics data
            for df, loc, api_name in valid_mm_results:
                if param in df.columns:
                    available_data.append((df, loc, api_name))
            
            # Add Stormglass data
            for df, loc, api_name in valid_sg_results:
                if param in df.columns:
                    available_data.append((df, loc, api_name))
            
            if not available_data:
                continue
            
            # Create the plot
            fig, ax = plt.subplots(figsize=figsize)
            
            # Define colors for different APIs
            api_colors = {
                "Visual Crossing": "blue",
                "Meteomatics": "red", 
                "Stormglass": "green"
            }
            
            for df, loc, api_name in available_data:
                lat, lon = loc
                location_label = f"{api_name} ({lat:.2f}, {lon:.2f})"
                
                ax.plot(
                    df["datetime"],
                    df[param],
                    label=location_label,
                    color=api_colors.get(api_name, "black"),
                    linewidth=2,
                    marker="o",
                    markersize=4,
                )
            
            ax.set_xlabel("Time")
            ax.set_ylabel(param)
            ax.set_title(f"{param} Comparison")
            ax.legend()
            if show_grid:
                ax.grid(True, alpha=0.3)
            
            # Format x-axis
            ax.xaxis.set_major_formatter(mdates.DateFormatter(time_format))
            ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
            
            plt.tight_layout()
            plt.show()

    def plot_forecasts_comparison(
        self,
        locations,
        start_datetime=None,
        end_datetime=None,
        unit_group="metric",
        model="mix",
        interval=None,
        meteomatics_interval=None,
        label_locations=True,
        figsize=(14, 4),
        time_format="%H:%M",
        date_format="%Y-%m-%d",
        show_grid=True,
    ):
        """
        Fetches forecasts from both APIs and plots all received weather parameters for comparison.

        Args:
            locations (list): List of (lat, lon) tuples.
            start_datetime (datetime, optional): Start datetime for the forecast.
            end_datetime (datetime, optional): End datetime for the forecast.
            unit_group (str): Unit group for Visual Crossing API (default: "metric").
            model (str, optional): Weather model for Meteomatics. Defaults to "mix".
            interval (timedelta, optional): Data interval for Meteomatics. Defaults to 1 hour.
            meteomatics_interval (timedelta, optional): Specific interval for Meteomatics API. Overrides interval if provided.
            label_locations (bool): Whether to label each line with its location.
            figsize (tuple): Figure size for each subplot.
            time_format (str): Format for time display on x-axis (default: "%H:%M").
            date_format (str): Format for date display on x-axis (default: "%Y-%m-%d").
            show_grid (bool): Whether to show grid on plots (default: True).
        """
        # Get data from both APIs
        try:
            vc_results = self.visualcrossing_api.get_forecast(
                locations,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                unit_group=unit_group,
            )
        except Exception as e:
            print(f"Error fetching Visual Crossing data: {e}")
            vc_results = []

        try:
            # Use meteomatics_interval if provided, otherwise fall back to interval
            mm_interval = meteomatics_interval if meteomatics_interval is not None else interval
            mm_results = self.meteomatics_api.get_forecast(
                locations,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                model=model,
                interval=mm_interval,
            )
        except Exception as e:
            print(f"Error fetching Meteomatics data: {e}")
            mm_results = []

        # Combine results with API labels
        all_results = []
        for i, (vc_df, mm_df, loc) in enumerate(zip(vc_results, mm_results)):
            if not vc_df.empty:
                vc_df = vc_df.copy()
                vc_df["api_source"] = "Visual Crossing"
                all_results.append((vc_df, loc, "Visual Crossing"))
            
            if not mm_df.empty:
                mm_df = mm_df.copy()
                mm_df["api_source"] = "Meteomatics"
                all_results.append((mm_df, loc, "Meteomatics"))

        if not all_results:
            print("No data available from either API.")
            return

        # Collect all parameter names (excluding metadata columns)
        metadata_cols = {
            "datetime",
            "latitude",
            "longitude",
            "api_source",
        }
        all_params = set()
        for df, _, _ in all_results:
            if not df.empty:
                all_params.update(
                    [col for col in df.columns if col not in metadata_cols]
                )

        # Only keep columns that are numeric or boolean for plotting
        safe_params = []
        skipped_params = []
        non_numeric_params = []

        for df, _, _ in all_results:
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

        # Plot each parameter as a separate subplot
        for param in safe_params:
            # Filter results that have this parameter
            param_results = [
                (df, loc, api_name) 
                for df, loc, api_name in all_results 
                if param in df.columns and not df.empty
            ]

            if not param_results:
                continue

            n_locations = len(set((loc[0], loc[1]) for _, loc, _ in param_results))
            fig, axes = plt.subplots(
                n_locations, 1, figsize=(figsize[0], figsize[1] * n_locations)
            )
            if n_locations == 1:
                axes = [axes]

            # Group by location
            location_groups = {}
            for df, loc, api_name in param_results:
                loc_key = (loc[0], loc[1])
                if loc_key not in location_groups:
                    location_groups[loc_key] = []
                location_groups[loc_key].append((df, api_name))

            for i, (loc_key, group_data) in enumerate(location_groups.items()):
                ax = axes[i]
                
                # Define title with coordinates
                title = f"Comparison of '{param}' for coordinates {loc_key[0]:.4f}, {loc_key[1]:.4f}"
                ax.set_title(title, fontsize=12, fontweight="bold")

                # Define time range for axis formatting
                all_timestamps = []
                for df, _ in group_data:
                    all_timestamps.extend(df["datetime"].tolist())
                time_range = max(all_timestamps) - min(all_timestamps)

                # Plot data from each API
                colors = ["blue", "red", "green", "orange", "purple"]
                for j, (df, api_name) in enumerate(group_data):
                    color = colors[j % len(colors)]
                    ax.plot(
                        df["datetime"],
                        df[param],
                        label=f"{api_name}",
                        linewidth=2,
                        marker="o",
                        markersize=4,
                        color=color,
                        alpha=0.8,
                    )

                ax.set_ylabel(param, fontsize=10, fontweight="bold")
                ax.legend(fontsize=9)

                # Grid settings
                if show_grid:
                    ax.grid(True, alpha=0.3, linestyle="--")

                # Time axis formatting
                if time_range > timedelta(hours=12):
                    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
                    ax.xaxis.set_major_locator(mdates.HourLocator(interval=4))
                    ax.xaxis.set_minor_locator(mdates.HourLocator(interval=1))
                else:
                    ax.xaxis.set_major_formatter(mdates.DateFormatter(time_format))
                    ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
                    ax.xaxis.set_minor_locator(mdates.MinuteLocator(interval=30))

                # Add time range label
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

                # Rotate labels for better readability
                plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")
                plt.setp(ax.xaxis.get_minorticklabels(), rotation=45, ha="right")

                # Add X-axis label
                ax.set_xlabel("Time", fontsize=10, fontweight="bold")

            # Automatic layout adjustment
            plt.tight_layout(rect=[0, 0.03, 1, 0.97])
            plt.show() 