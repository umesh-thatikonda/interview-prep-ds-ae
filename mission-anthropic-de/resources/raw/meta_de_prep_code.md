# Meta DE Prep — Raw Questions (from session paste)

## SOURCE 1: Python Problems (FS1–FS6)

### FS1 Streaming — Q1: Average Rating per Content
Given a stream of user ratings as list of tuples (user_id, content_name, genre, rating), calculate the average rating for each content_name. Return a dictionary mapping content_name to average rating.

stream = [
    (1001, "The Office", "Comedy", 3.0),
    (1002, "The Godfather", "Drama", 4.0),
    (1003, "The Notebook", "Romance", 5.0),
    (1043, "Her", "Sci-Fi", 3.0),
    (1004, "Interstellar", "Sci-Fi", 2.0),
    (1011, "Breaking Bad", "Drama", 4.0),
    (1032, "Interstellar", "Sci-Fi", 3.0),
    (1124, "Big Bang Theory", "Comedy", 1.0),
    (1011, "The Godfather", "Drama", 3.0),
    (1052, "Interstellar", "Sci-Fi", 3.0),
    (1226, "Big Bang Theory", "Comedy", 5.0),
    (2164, "The Martian", "Sci-Fi", 4.0),
    (2451, "Back To The Future", "Sci-Fi", 4.0)
]

Expected Output:
{'The Office': 3.0, 'The Godfather': 3.5, 'The Notebook': 5.0, 'Her': 3.0,
 'Interstellar': 2.666..., 'Breaking Bad': 4.0, 'Big Bang Theory': 3.0,
 'The Martian': 4.0, 'Back To The Future': 4.0}

---

### FS1 Streaming — Q2: Top 2 Videos by Genre
Merge two datasets using title key. Group videos by genre. For each genre, identify top 2 videos with highest rating. Print formatted output.

video_ratings_lengths = [
    {"title": "Ultimate Guide to Digital Marketing", "rating": 4.5, "video_length_mins": 50},
    {"title": "Investing 101 Crash Course", "rating": 4.8, "video_length_mins": 70},
    {"title": "2023 Crash Course", "rating": 4.2, "video_length_mins": 35},
    {"title": "React Crash Course", "rating": 4.7, "video_length_mins": 45},
    {"title": "Rust Crash Course", "rating": 4.9, "video_length_mins": 60},
    {"title": "Weekly Yoga Routine for Beginners", "rating": 4.3, "video_length_mins": 25},
    {"title": "10-Minute Meditation for Stress Reller", "rating": 4.1, "video_length_mins": 10},
    {"title": "Standup Comedy Special", "rating": 4.9, "video_length_mins": 60},
    {"title": "Funny Animal Fails Compilation", "rating": 4.6, "video_length_mins": 15},
    {"title": "How to Make Your Friends Laugh", "rating": 4.2, "video_length_mins": 6},
]

video_genres_watch = [
    {"title": "Ultimate Guide to Digital Marketing", "genre": "Business", "watch_duration_mins": 45},
    {"title": "Investing 101 Crash Course", "genre": "Business", "watch_duration_mins": 50},
    {"title": "2023 Crash Course", "genre": "Business", "watch_duration_mins": 40},
    {"title": "React Crash Course", "genre": "Tech", "watch_duration_mins": 50},
    {"title": "Rust Crash Course", "genre": "Tech", "watch_duration_mins": 55},
    {"title": "Weekly Yoga Routine for Beginners", "genre": "Lifestyle", "watch_duration_mins": 20},
    {"title": "10-Minute Meditation for Stress Reller", "genre": "Lifestyle", "watch_duration_mins": 15},
    {"title": "Standup Comedy Special", "genre": "Comedy", "watch_duration_mins": 15},
    {"title": "Funny Animal Fails Compilation", "genre": "Comedy", "watch_duration_mins": 15},
    {"title": "How to Make Your Friends Laugh", "genre": "Comedy", "watch_duration_mins": 6},
]

Expected Output:
Top 2 videos in genre 'Business': Investing 101 Crash Course (4.8), Ultimate Guide to Digital Marketing (4.5)
Top 2 videos in genre 'Tech': Rust Crash Course (4.9), React Crash Course (4.7)
Top 2 videos in genre 'Lifestyle': Weekly Yoga Routine for Beginners (4.3), 10-Minute Meditation (4.1)
Top 2 videos in genre 'Comedy': Standup Comedy Special (4.9), Funny Animal Fails Compilation (4.6)

---

### FS2 Newsfeed — Q1: Queue Buffer Processing
A queue is a buffer/cache with fixed size. Assuming buffer holds only 3 events, write a function that calculates and outputs number of engagements, total time spent (in seconds), and post ids in the buffer every time buffer is full. Ignore test content.

stream = [
   {'post_id': 101, 'viewed_time_ms': 6500, 'engaged_with_post': 1},
   {'post_id': 104, 'viewed_time_ms': 200, 'engaged_with_post': 1},
   {'post_id': 105, 'viewed_time_ms': 4200, 'engaged_with_post': 0, 'is_test_content': True},
   {'post_id': 108, 'viewed_time_ms': 4499, 'engaged_with_post': 1},
   {'post_id': 105, 'viewed_time_ms': 500, 'engaged_with_post': 1},
]

Expected Output:
You've got 2 engagements and spent 6.700s viewing content. Post ids: 101, 104
You've got 2 engagements and spent 4.699s viewing content. Post ids: 104, 108
You've got 2 engagements and spent 4.999s viewing content. Post ids: 108, 105

---

### FS4 Instamart — Q1: Delivery Time Estimation
Given routing_steps (TRAVEL/PICKUP/DROPOFF) and a travel_time matrix, create a map of order_id to expected delivery time.

routing_steps = [
  {"driver_id": 1, "action": "TRAVEL",  "location_id": 1},
  {"driver_id": 2, "action": "TRAVEL",  "location_id": 3},
  {"driver_id": 1, "action": "PICKUP",  "order_id": 1},
  {"driver_id": 1, "action": "TRAVEL",  "location_id": 2},
  {"driver_id": 2, "action": "PICKUP",  "order_id": 2},
  {"driver_id": 2, "action": "PICKUP",  "order_id": 3},
  {"driver_id": 2, "action": "TRAVEL",  "location_id": 4},
  {"driver_id": 2, "action": "PICKUP",  "order_id": 4},
  {"driver_id": 1, "action": "DROPOFF", "order_id": 1},
  {"driver_id": 1, "action": "TRAVEL",  "location_id": 5},
  {"driver_id": 2, "action": "DROPOFF", "order_id": 4},
  {"driver_id": 2, "action": "DROPOFF", "order_id": 2},
  {"driver_id": 2, "action": "TRAVEL",  "location_id": 1},
  {"driver_id": 2, "action": "DROPOFF", "order_id": 3}
]

travel_time = [
  [ 0, 10, 17, 23,  9, 13 ],
  [10,  0, 30, 25,  5, 25 ],
  [17, 30,  0, 10,  7, 33 ],
  [23, 25, 10,  0, 11, 35 ],
  [ 9,  5,  7, 11,  0, 27 ],
  [13, 25, 33, 35, 27,  0 ]
]

Output format: "order <order_id> is expected to be delivered at <time>"

---

### FS3 Carpool — Q1: Vehicle Capacity Feasibility
Given total passenger capacity and list of booking requests, return True/False whether all bookings can be accommodated.

vehicle_passenger_capacity = 6
booking_requests = [
    {"request_id": 1, "pickup_time": 10, "drop_off_time": 20, "total_passengers": 3},
    {"request_id": 2, "pickup_time": 15, "drop_off_time": 25, "total_passengers": 2},
    {"request_id": 3, "pickup_time": 30, "drop_off_time": 40, "total_passengers": 3},
    {"request_id": 4, "pickup_time": 35, "drop_off_time": 45, "total_passengers": 1},
    {"request_id": 5, "pickup_time": 50, "drop_off_time": 60, "total_passengers": 4},
    {"request_id": 6, "pickup_time": 55, "drop_off_time": 80, "total_passengers": 1},
    {"request_id": 7, "pickup_time": 70, "drop_off_time": 85, "total_passengers": 3},
    {"request_id": 8, "pickup_time": 75, "drop_off_time": 100, "total_passengers": 1},
    {"request_id": 9, "pickup_time": 90, "drop_off_time": 100, "total_passengers": 1}
]
Expected: True

---

### FS5 Twitter — Q1: Reciprocal Followers + Recommendations
For each user, provide count of reciprocal followers and follow recommendations.

user_follows = {
    'A': ['B', 'C', 'D'],
    'B': ['D', 'E', 'F', 'A'],
    'C': ['F', 'A', 'E'],
    'D': []
}

Expected:
{'A': [2, ['E', 'F']], 'B': [1, ['C']], 'C': [1, ['B', 'D']], 'D': [0, ['A', 'B', 'C', 'E', 'F']]}

---

### FS6 Car Rental — Q1: Single Car Booking Conflict
Given ordered dict of rental orders, accept or reject each order for a single car. Process in order_id order.

rental_orders_by_id = {
    21: {"pickup_date": "2025-03-01", "dropoff_date": "2025-03-07"},
    22: {"pickup_date": "2025-03-10", "dropoff_date": "2025-03-13"},
    23: {"pickup_date": "2025-03-02", "dropoff_date": "2025-03-11"},
    24: {"pickup_date": "2025-03-08", "dropoff_date": "2025-03-28"},
    25: {"pickup_date": "2025-02-15", "dropoff_date": "2025-02-25"},
    26: {"pickup_date": "2025-02-27", "dropoff_date": "2025-02-28"},
    27: {"pickup_date": "2025-03-10", "dropoff_date": "2025-03-31"},
    28: {"pickup_date": "2025-03-13", "dropoff_date": "2025-03-30"},
    29: {"pickup_date": "2025-03-14", "dropoff_date": "2025-03-30"},
}

Expected: 21 ok, 22 ok, 23 rejected, 24 rejected, 25 ok, 26 ok, 27 rejected, 28 rejected, 29 ok

---

## SOURCE 2: SQL Problems (FS1–FS6)

### FS1 Streaming — SQL Q1
For content watched more than 10 minutes cumulative, list content_ids with num_viewers and total_view_time.

Tables: fct_view_logs (session_id, user_id, content_id, start_ts, end_ts, date), dim_content (content_id, content_type, title, genre, studio_id, creation_date, date)

### FS1 Streaming — SQL Q2
Update cumulative_studio_metrics table daily with total_view_time_1d and total_view_time_lifetime per studio.

### FS2 Newsfeed — SQL Q1
Limited to today, for video content: % of video views that reach full duration, total time watching videos, average view length.

Tables: fct_newsfeed_action (date, user_id, session_id, content_id, action_id, action_type, view_start, view_end), dim_content (date, content_id, content_type, creator_id, creation_date, video_length_seconds)

### FS2 Newsfeed — SQL Q2
What percentage of content generated today had a reaction but no comments today?

### FS3 Carpool — SQL Q1
Ratio of carpool to all trips in last 30 days.

Tables: fct_rides (ride_id, user_id, driver_id, vehicle_id, ride_type, region, start_time, end_time, date), dim_vehicle (vehicle_id, type, capacity)

### FS3 Carpool — SQL Q2
Rank vehicle types by most used for pool, ranking based on customer time spent in vehicle.

### FS3 Carpool — SQL Q3
Number of drivers who made more pool rides than regular rides.

### FS4 Instamart — SQL Q1
Average order value per user's city today.

Tables: dim_users, dim_stores, fct_order (order_id, store_id, driver_id, user_id, order_type, item_count, order_placed_ts, order_received_ts, store_accept_ts, order_prep_start_ts, order_ready_ts, driver_pickup_ts, order_delivery_ts, cancelled_by, cancellation_reason, net_order_amount, date)

### FS4 Instamart — SQL Q2
How many stores do more pickups than deliveries today?

### FS4 Instamart — SQL Q3
Top 3 most popular cuisines last month by number of customers.

### FS5 Twitter — SQL Q1
For users who went private in last 7 days, find overall acceptance rate.

Tables: Dim_user (User_id, privacy_setting, privacy_setting_last_update_ts, date), Fct_follows (Source_user, Target_user, action [follow_request/follow_accept/follow_success/reject/unfollow], action_ts)

### FS5 Twitter — SQL Q2
For requesters who went private in last 7 days: count of requesters, avg/min/max acceptance rate per requester.

### FS5 Twitter — SQL Q3
Create dim_follows table: date, source, target, valid_from from fct_follows.

### FS5 Twitter — SQL Q4
Create dim_follows with is_reciprocal flag.

### FS6 Car Rental — SQL Q1
For rentals picked up in California last year by car size: total reservations, distinct users, ratio (avg rentals per user).

Tables: dim_cars (car_id, make, model, year, size, license_plate_number, location_id), dim_locations (location_id, address, city, state, zip, country, type), dim_users (user_id, name, email, license_number, license_state), fct_rentals (rental_id, user_id, car_id, pickup_location_id, dropoff_location_id, pickup_time, dropoff_time, rate_per_day)

### FS6 Car Rental — SQL Q2
Utilization rate = cars rented / cars in location, by car size.

---

## SOURCE 3: Artifact Content

### YouTube — Python Q1: Streaming Genre Average
Process youtube_video_views row by row; after each row print genre and running average watch_duration_mins per genre.

youtube_video_views = [
    {"title": "Ultimate Guide to Digital Marketing", "video_length_mins": 50, "genre": "Business", "watch_duration_mins": 45},
    {"title": "Investing 101 Crash Course", "video_length_mins": 70, "genre": "Business", "watch_duration_mins": 50},
    {"title": "React Crash Course", "video_length_mins": 60, "genre": "Technology", "watch_duration_mins": 55},
    {"title": "Rust Crash Course", "video_length_mins": 60, "genre": "Technology", "watch_duration_mins": 50},
    {"title": "Weekly Yoga Routine for Beginners", "video_length_mins": 25, "genre": "Lifestyle", "watch_duration_mins": 20},
    {"title": "10-Minute Meditation for Stress Relief", "video_length_mins": 10, "genre": "Lifestyle", "watch_duration_mins": 15},
    {"title": "Standup Comedy Special", "video_length_mins": 60, "genre": "Comedy", "watch_duration_mins": 15},
    {"title": "Funny Animal Fails Compilation", "video_length_mins": 15, "genre": "Comedy", "watch_duration_mins": 15},
    {"title": "How to Make Your Friends Laugh", "video_length_mins": 6, "genre": "Comedy", "watch_duration_mins": 6}
]

### YouTube — SQL Q1
Videos watched > 18 minutes today: list video_ids, num_viewers, total_watch_time_mins.

### YouTube — SQL Q2
Channel cumulative metrics: viewers_1d, sessions_1d, viewers_ltv, sessions_ltv.
Formula: viewers_ltv(today) = viewers_ltv(yesterday) + viewers_1d(today)

### DoorDash — SQL Q1
Rank vehicle types by most used for pooled deliveries (by time spent).

### DoorDash — SQL Q2
Dashboard aggregation table: region × vehicle_type × delivery_date with ratio of pooled to all deliveries. Use GROUPING SETS.

### Instagram Stories — SQL Q1
% of video Story views that reached full duration today. Also total time and avg view time.

Tables: fct_story_interaction_di (date, user_id, session_id, story_id, action_id, action_type, view_start, view_end), dim_story (date, story_id, story_type, creator_id, creation_date, story_length_seconds)

### Instagram Stories — SQL Q2
% of Stories created today that had a reaction but no replies today.

### Instagram Stories — SQL Q3 (extra)
Count of users active on Newsfeed in last rolling 30 days.

### Instagram Stories — SQL Q4 (extra)
Count of users who reacted exclusively to media (photo or video) today — no reactions to non-media content.
