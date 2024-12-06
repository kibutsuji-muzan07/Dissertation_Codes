def fixed_wing_analyzer(log_file):
    # Connect to the log file
    mavlog = mavutil.mavlink_connection(log_file)

    # Initialize empty DataFrames
    gps_df = pd.DataFrame()
    flight_mode_df = pd.DataFrame()
    terrain_df = pd.DataFrame()
    cmd_df = pd.DataFrame()

    # Reading data from the log file
    try:
        while True:
            msg = mavlog.recv_match(blocking=False)
            if msg is None:
                break

            # Process GPS data (GPS messages)
            if msg.get_type() == 'GPS':
                try:
                    gps_df = pd.concat([gps_df, pd.DataFrame({
                        'timestamp': [msg.TimeUS / 1e6],
                        'lat': [msg.Lat],
                        'lng': [msg.Lng],
                        'gps_height': [msg.Alt]
                    })], ignore_index=True)
                except ValueError as gps_error:
                    print(f"Skipping GPS message due to error: {gps_error}")


            # Process terrain data (TERR messages)
            if msg.get_type() == 'TERR':
                try:
                    terrain_df = pd.concat([terrain_df, pd.DataFrame({
                        'timestamp': [msg.TimeUS / 1e6],
                        'lat': [msg.Lat],
                        'lng': [msg.Lng],
                        'terrain_height': [msg.TerrH]
                    })], ignore_index=True)
                except ValueError as terr_error:
                    print(f"Skipping TERR message due to error: {terr_error}")

            # Process lidar data (MODE messages)
            if msg.get_type() == 'MODE':
                try:
                    flight_mode_df = pd.concat([flight_mode_df, pd.DataFrame({
                        'timestamp': [msg.TimeUS / 1e6],
                        'mode': [msg.Mode],
                        'modeNum': [msg.ModeNum]
                    })], ignore_index=True)
                except ValueError as flight_mode_error:
                    print(f"Skipping MODE message due to error: {flight_mode_error}")

            # Process Command data for waypoints
            if msg.get_type() == 'CMD':

                try:
                  cmd_df = pd.concat([cmd_df, pd.DataFrame({
                      'timestamp': [msg.TimeUS / 1e6],
                      'CNum': [msg.CNum],      # Use 'get' to avoid KeyErrors if column missing
                      'CId': [msg.CId],
                      'lat': [msg.Lat],
                      'lng': [msg.Lng],
                      'alt': [msg.Alt]
                  })], ignore_index=True)
                except ValueError as rfnd_error:
                    print(f"Skipping RFND message due to error: {rfnd_error}")

        # Interpolating data where necessary
        # Interpolating terrain height to gps timestamps
        if not terrain_df.empty and not gps_df.empty:
            try:
                terrain_interp_height = interp1d(terrain_df['timestamp'], terrain_df['terrain_height'], kind='nearest', fill_value="extrapolate")
                terrain_interp_lat = interp1d(terrain_df['timestamp'], terrain_df['lat'], kind='nearest', fill_value="extrapolate")
                terrain_interp_lng = interp1d(terrain_df['timestamp'], terrain_df['lng'], kind='nearest', fill_value="extrapolate")
                gps_df['terrain_height'] = terrain_interp_height(gps_df['timestamp'])
                gps_df['terrain_lat'] = terrain_interp_lat(gps_df['timestamp'])
                gps_df['terrain_lng'] = terrain_interp_lng(gps_df['timestamp'])
            except ValueError as terrain_error:
                print(f"Error interpolating terrain data: {terrain_error}")

    # # Sort both DataFrames by timestamp (necessary for merge_asof)
    # gps_df = gps_df.sort_values('timestamp')
    # flight_mode_df = flight_mode_df.sort_values('timestamp')

    # # Use merge_asof with direction='backward' to fill mode in gps_df based on the last mode up to each timestamp
    # gps_df['mode'] = pd.merge_asof(gps_df, flight_mode_df, on='timestamp', direction='backward')['mode']

        return gps_df, cmd_df

    except Exception as general_error:
      print(f"An unexpected error occurred: {general_error}")