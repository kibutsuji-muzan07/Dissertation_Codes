def calculate_laser_angle(roll_angle, pitch_angle):

    # Convert angles to radians
    roll_rad = np.radians(roll_angle)
    pitch_rad = np.radians(pitch_angle)

    # Define the initial laser vector (pointing straight down)
    initial_v = np.array([0, 0, -1])

    # Roll Rotation Matrix (rotation around the x-axis)
    R_roll = np.array([
        [1, 0, 0],
        [0, np.cos(roll_rad), -np.sin(roll_rad)],
        [0, np.sin(roll_rad), np.cos(roll_rad)]
    ])

    # Pitch Rotation Matrix (rotation around the y-axis)
    R_pitch = np.array([
        [np.cos(pitch_rad), 0, np.sin(pitch_rad)],
        [0, 1, 0],
        [-np.sin(pitch_rad), 0, np.cos(pitch_rad)]
    ])

    # Apply the roll and then the pitch to the initial laser vector
    # Step 1: Apply roll
    rolled_v = np.dot(R_roll, initial_v)

    # Step 2: Apply pitch
    final_v = np.dot(R_pitch, rolled_v)

    # Calculate the angle between the initial vector and final vector
    cos_theta = np.dot(initial_v, final_v) / (np.linalg.norm(initial_v) * np.linalg.norm(final_v))
    final_angle = np.arccos(cos_theta)

    # Convert to degrees for easier interpretation
    final_angle_degrees = np.degrees(final_angle)
    return final_angle_degrees
