def NED_to_ENU(N, E, D):

  E_enu = N
  N_enu = E
  U_enu = - D
  return E_enu, N_enu, U_enu



def analyzer(log_file):
    # Connect to the log file
    mavlog = mavutil.mavlink_connection(log_file)

    # Initialize empty DataFrames
    terrain_df = pd.DataFrame()
    lidar_df = pd.DataFrame()
    gps_df = pd.DataFrame()
    att_df = pd.DataFrame()
    ned_df = pd.DataFrame()
    cmd_df = pd.DataFrame()
    mode_df = pd.DataFrame()

    # Reading data from the log file
    try:
        while True:
            msg = mavlog.recv_match(blocking=False)
            if msg is None:
                break

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

            # Process lidar data (RFND messages)
            if msg.get_type() == 'RFND':
                try:
                    lidar_df = pd.concat([lidar_df, pd.DataFrame({
                        'timestamp': [msg.TimeUS / 1e6],
                        'lidar_height': [msg.Dist]
                    })], ignore_index=True)
                except ValueError as rfnd_error:
                    print(f"Skipping RFND message due to error: {rfnd_error}")

            # Process mode data (MODE messages)
            if msg.get_type() == 'MODE':
                try:
                    mode_df = pd.concat([mode_df, pd.DataFrame({
                        'timestamp': [msg.TimeUS / 1e6],
                        'mode': [msg.Mode]
                    })], ignore_index=True)
                except ValueError as mode_error:
                    print(f"Skipping MODE message due to error: {mode_error}")

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

            # Process GPS data (POS messages)
            if msg.get_type() == 'POS':
                try:
                    gps_df = pd.concat([gps_df, pd.DataFrame({
                        'timestamp': [msg.TimeUS / 1e6],
                        'gps_height': [msg.Alt],
                        'gps_lat': [msg.Lat],
                        'gps_lng': [msg.Lng]
                    })], ignore_index=True)
                except ValueError as pos_error:
                    print(f"Skipping POS message due to error: {pos_error}")

            # Process NED data (SRTL messages)
            if msg.get_type() == 'SRTL':
                try:
                    ned_df = pd.concat([ned_df, pd.DataFrame({
                        'timestamp': [msg.TimeUS / 1e6],
                        'N': [msg.N],
                        'E': [msg.E],
                        'D': [msg.D]
                    })], ignore_index=True)
                except ValueError as ned_error:
                    print(f"Skipping POS message due to error: {ned_error}")

            # Process attitude data (ATT messages)
            if msg.get_type() == 'ATT':
                try:
                    att_df = pd.concat([att_df, pd.DataFrame({
                        'timestamp': [msg.TimeUS / 1e6],
                        'roll': [msg.Roll],
                        'pitch': [msg.Pitch],
                        'yaw':[msg.Yaw]
                    })], ignore_index=True)
                except ValueError as att_error:
                    print(f"Skipping ATT message due to error: {att_error}")

        # Step 1: Remove the first row
        lidar_df = lidar_df.iloc[1:]

    except Exception as general_error:
        print(f"An unexpected error occurred: {general_error}")



    # Step 2: Remove every other row, effectively halving the number of rows
    # lidar_df = lidar_df.iloc[::2].reset_index(drop=True)

    # Interpolating data where necessary

    # Interpolating ned data to gps timestamp
    if not ned_df.empty and not lidar_df.empty:
        try:
            ned_interp_n = interp1d(ned_df['timestamp'], ned_df['N'], kind = 'nearest', fill_value="extrapolate")
            lidar_df['N'] = ned_interp_n(lidar_df['timestamp'])
            ned_interp_e = interp1d(ned_df['timestamp'], ned_df['E'], kind = 'nearest', fill_value="extrapolate")
            lidar_df['E'] = ned_interp_e(lidar_df['timestamp'])
            ned_interp_d = interp1d(ned_df['timestamp'], ned_df['D'], kind = 'nearest', fill_value="extrapolate")
            lidar_df['D'] = ned_interp_d(lidar_df['timestamp'])
        except ValueError as ned_error:
            print(f"Error interpolating LIDAR data: {ned_error}")
    else:
        try:
            ned_interp_n = interp1d(ned_df['timestamp'], ned_df['N'], kind = 'nearest', fill_value="extrapolate")
            gps_df['N'] = ned_interp_n(gps_df['timestamp'])
            ned_interp_e = interp1d(ned_df['timestamp'], ned_df['E'], kind = 'nearest', fill_value="extrapolate")
            gps_df['E'] = ned_interp_e(gps_df['timestamp'])
            ned_interp_d = interp1d(ned_df['timestamp'], ned_df['D'], kind = 'nearest', fill_value="extrapolate")
            gps_df['D'] = ned_interp_d(gps_df['timestamp'])
        except ValueError as ned_error:
            print(f"Error interpolating LIDAR data: {ned_error}")


     # Interpolating lidar height to gps timestamps
    if not lidar_df.empty and not gps_df.empty:
        try:
            gps_interp_height = interp1d(gps_df['timestamp'], gps_df['gps_height'], kind = 'nearest', fill_value="extrapolate")
            gps_interp_lat = interp1d(gps_df['timestamp'], gps_df['gps_lat'], kind = 'nearest', fill_value="extrapolate")
            gps_interp_lng = interp1d(gps_df['timestamp'], gps_df['gps_lng'], kind = 'nearest', fill_value="extrapolate")
            lidar_df['gps_height'] = gps_interp_height(lidar_df['timestamp'])
            lidar_df['gps_lat'] = gps_interp_lat(lidar_df['timestamp'])
            lidar_df['gps_lng'] = gps_interp_lng(lidar_df['timestamp'])
        except ValueError as gps_error:
            print(f"Error interpolating GPS data: {gps_error}")

    # Interpolating terrain height to gps timestamps
    if not terrain_df.empty and not lidar_df.empty:
        try:
            terrain_interp_height = interp1d(terrain_df['timestamp'], terrain_df['terrain_height'], kind='nearest', fill_value="extrapolate")
            terrain_interp_lat = interp1d(terrain_df['timestamp'], terrain_df['lat'], kind='nearest', fill_value="extrapolate")
            terrain_interp_lng = interp1d(terrain_df['timestamp'], terrain_df['lng'], kind='nearest', fill_value="extrapolate")
            lidar_df['terrain_height'] = terrain_interp_height(lidar_df['timestamp'])
            lidar_df['terrain_lat'] = terrain_interp_lat(lidar_df['timestamp'])
            lidar_df['terrain_lng'] = terrain_interp_lng(lidar_df['timestamp'])

        except ValueError as terrain_error:
            print(f"Error interpolating terrain data: {terrain_error}")

    else:
        try:
            terrain_interp_height = interp1d(terrain_df['timestamp'], terrain_df['terrain_height'], kind='nearest', fill_value="extrapolate")
            terrain_interp_lat = interp1d(terrain_df['timestamp'], terrain_df['lat'], kind='nearest', fill_value="extrapolate")
            terrain_interp_lng = interp1d(terrain_df['timestamp'], terrain_df['lng'], kind='nearest', fill_value="extrapolate")
            gps_df['terrain_height'] = terrain_interp_height(gps_df['timestamp'])
            gps_df['terrain_lat'] = terrain_interp_lat(gps_df['timestamp'])
            gps_df['terrain_lng'] = terrain_interp_lng(gps_df['timestamp'])
        except ValueError as ned_error:
            print(f"Error interpolating LIDAR data: {terrain_error}")


    if not cmd_df.empty and not lidar_df.empty:
        try:
            lidar_df['cmd_lat'] = pd.merge_asof(lidar_df, cmd_df, on='timestamp', direction='backward')['lat']
            lidar_df['cmd_lng'] = pd.merge_asof(lidar_df, cmd_df, on='timestamp', direction='backward')['lng']
            lidar_df['cmd_alt'] = pd.merge_asof(lidar_df, cmd_df, on='timestamp', direction='backward')['alt']
            lidar_df['cmd_cnum'] = pd.merge_asof(lidar_df, cmd_df, on='timestamp', direction='backward')['CNum']
            lidar_df['cmd_cid'] = pd.merge_asof(lidar_df, cmd_df, on='timestamp', direction='backward')['CId']
        except ValueError as cmd_error:
            print(f"Error interpolating cmd data: {cmd_error}")

    else:
        try:
            gps_df['cmd_lat'] = pd.merge_asof(gps_df, cmd_df, on='timestamp', direction='backward')['lat']
            gps_df['cmd_lng'] = pd.merge_asof(gps_df, cmd_df, on='timestamp', direction='backward')['lng']
            gps_df['cmd_alt'] = pd.merge_asof(gps_df, cmd_df, on='timestamp', direction='backward')['alt']
            gps_df['cmd_cnum'] = pd.merge_asof(gps_df, cmd_df, on='timestamp', direction='backward')['CNum']
            gps_df['cmd_cid'] = pd.merge_asof(gps_df, cmd_df, on='timestamp', direction='backward')['CId']
        except ValueError as cmd_error:
            print(f"Error interpolating cmd data: {cmd_error}")

    # Interpolating roll, pitch and yaw to gps timestamps
    if not att_df.empty and not lidar_df.empty:
        try:
            roll_interp = interp1d(att_df['timestamp'], att_df['roll'], kind='nearest', fill_value="extrapolate")
            pitch_interp = interp1d(att_df['timestamp'], att_df['pitch'], kind='nearest', fill_value="extrapolate")
            yaw_interp = interp1d(att_df['timestamp'], att_df['yaw'], kind='nearest', fill_value="extrapolate")
            lidar_df['roll'] = roll_interp(lidar_df['timestamp'])
            lidar_df['pitch'] = pitch_interp(lidar_df['timestamp'])
            lidar_df['yaw'] = yaw_interp(lidar_df['timestamp'])
        except ValueError as att_error:
            print(f"Error interpolating attitude data: {att_error}")

    else:
        try:
            roll_interp = interp1d(att_df['timestamp'], att_df['roll'], kind='nearest', fill_value="extrapolate")
            pitch_interp = interp1d(att_df['timestamp'], att_df['pitch'], kind='nearest', fill_value="extrapolate")
            yaw_interp = interp1d(att_df['timestamp'], att_df['yaw'], kind='nearest', fill_value="extrapolate")
            gps_df['roll'] = roll_interp(gps_df['timestamp'])
            gps_df['pitch'] = pitch_interp(gps_df['timestamp'])
            gps_df['yaw'] = yaw_interp(gps_df['timestamp'])
        except ValueError as att_error:
            print(f"Error interpolating attitude data: {att_error}")

        # Interpolate mode data with LIDAR timestamps
    if not mode_df.empty and not lidar_df.empty:
        try:
            lidar_df['mode'] = pd.merge_asof(lidar_df, mode_df, on='timestamp', direction='backward')['mode']

        except ValueError as interpolation_error:
            print(f"Error interpolating mode data with LIDAR data: {interpolation_error}")

    else:
        try:
            gps_df['mode'] = pd.merge_asof(gps_df, mode_df, on='timestamp', direction='backward')['mode']
        except ValueError as interpolation_error:
            print(f"Error interpolating mode data with LIDAR data: {interpolation_error}")

    # Corrected lidar distance considering roll and pitch
    if 'roll' in lidar_df and 'pitch' in lidar_df and 'yaw' in lidar_df:
        lidar_df['deviated_angle'] = lidar_df.apply(
        lambda row: calculate_laser_angle(row['roll'], row['pitch']), axis=1)

        lidar_df['corrected_lidar_height'] = lidar_df['lidar_height'] * np.cos(np.radians(lidar_df['deviated_angle']))

        # Calculate horizontal displacement
        lidar_df['horizontal_displacement'] = lidar_df['lidar_height'] * np.sin(np.radians(lidar_df['deviated_angle']))

        # Calculate North and East adjustments
        lidar_df['Delta_N'] = lidar_df['horizontal_displacement'] * np.cos(np.radians(lidar_df['yaw']))
        lidar_df['Delta_E'] = lidar_df['horizontal_displacement'] * np.sin(np.radians(lidar_df['yaw']))

        # Update North and East positions
        lidar_df['N'] = lidar_df['N'] + lidar_df['Delta_N']
        lidar_df['E'] = lidar_df['E'] + lidar_df['Delta_E']

        # Convert NED to ENU
        lidar_df['E_enu'], lidar_df['N_enu'], lidar_df['U_enu'] = zip(*lidar_df.apply(lambda row: NED_to_ENU(row['N'], row['E'], row['D']), axis=1))

        # Reference point in lat, lon, and altitude
        lat_ref = lidar_df['gps_lat'].iloc[0]  # Reference latitude in degrees
        lon_ref = lidar_df['gps_lng'].iloc[0]  # Reference longitude in degrees

        # Convert lat, lon to a local UTM or Cartesian system
        proj_utm = Proj(proj="utm", zone=30, ellps="WGS84")
        lon_ref, lat_ref = proj_utm(lon_ref, lat_ref)

        # Calculate the final UTM coordinates
        lidar_df['lng_utm'] = lon_ref + lidar_df['N_enu']
        lidar_df['lat_utm'] = lat_ref + lidar_df['E_enu']


        # Convert UTM back to lat/lon for final coordinates
        lidar_df['lng_enu'], lidar_df['lat_enu'] = proj_utm(lidar_df['lng_utm'], lidar_df['lat_utm'], inverse=True)

        # Apply offset to align lidar with terrain heights
        lidar_df['adjusted_drone_height'] = lidar_df['corrected_lidar_height'] + lidar_df['terrain_height']

        lidar_df['terrain_trace_from_lidar'] = lidar_df['gps_height'] - lidar_df['corrected_lidar_height']
        return lidar_df, cmd_df

    else:
        return gps_df, cmd_df
