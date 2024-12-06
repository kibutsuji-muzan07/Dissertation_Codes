def custom_obs_data_segregator(start_timestamp, end_timestamp, bin_file):
    # Load the original .bin file
    mavlog = mavutil.mavlink_connection(r'{}'.format(bin_file), dialect='ardupilotmega')

    # Define start and end timestamps for altitude 8 sequence
    # start_timestamp = 143.463601  # replace with your start timestamp
    # end_timestamp = 381.919287    # replace with your end timestamp

    # Define start and end timestamps for altitude 6 sequence
    # start_timestamp = 381.919287  # replace with your start timestamp
    # end_timestamp = 618.098624    # replace with your end timestamp



    # Open a CSV file to save the filtered data
    with open(r'custom_obstacle.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['timestamp', 'message_type', 'fields'])  # Header row

        while True:
            try:
                msg = mavlog.recv_match()
                if msg is None:
                    break

                # Check if the message contains a timestamp and is within the range
                if hasattr(msg, 'TimeUS'):
                    timestamp = msg.TimeUS / 1e6  # Convert to seconds
                    if start_timestamp <= timestamp <= end_timestamp:
                        csvwriter.writerow([timestamp, msg.get_type(), msg.to_dict()])

            except Exception as e:
                print(f"Error processing message: {e}")
                continue