# Golden Python Questions — Meta/Anthropic DE Prep

> [!IMPORTANT]
> **Interviewer Availability:** The Safeguards team will be in NYC for an Offsite (May 5–9) and Hackweek (May 10–15). They are unavailable for interviews during this period.


Compiled from:
- `raw/meta_de_prep_code.md` (sources referred to as FS1–FS6 and YouTube Artifact)
- `raw/meta_tech_screen_v14.txt` (sources referred to as Meta Screen Set1, Set2, Set3, Set4, Additional)

Questions only (no solutions). Deduplicated across sources; each question lists all sources it appeared in.

---

## Q1: Average Rating per Content (Streaming)
**Source:** FS1 Streaming — Q1 (meta_de_prep_code.md)
**Pattern:** Running/overall average on a stream of tuples, group by key
**Difficulty:** Easy
**Time target:** 10 mins

### Problem Statement
Given a stream of user ratings as a list of tuples of the form `(user_id, content_name, genre, rating)`, calculate the average rating for each `content_name`. Return a dictionary mapping `content_name` to its average rating.

### Input
List of tuples: `(user_id, content_name, genre, rating)`

Sample:
```python
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
    (2451, "Back To The Future", "Sci-Fi", 4.0),
]
```

### Expected Output
```python
{'The Office': 3.0, 'The Godfather': 3.5, 'The Notebook': 5.0, 'Her': 3.0,
 'Interstellar': 2.666..., 'Breaking Bad': 4.0, 'Big Bang Theory': 3.0,
 'The Martian': 4.0, 'Back To The Future': 4.0}
```

### Notes
- Consider streaming/online average (maintain running sum + count) versus two-pass.
- Variant: compute per genre instead of per content.
- Edge cases: empty stream, duplicate `(user, content)` pairs.

---

## Q2: Top 2 Videos per Genre (Merge Two Datasets)
**Source:** FS1 Streaming — Q2 (meta_de_prep_code.md)
**Pattern:** Dict-to-dict join on title + group by genre + top-N per group by rating
**Difficulty:** Medium
**Time target:** 20 mins

### Problem Statement
Merge two datasets using the `title` key. Group the merged videos by `genre`. For each genre, identify the top 2 videos with the highest `rating`. Print a formatted line per genre.

### Input
```python
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
```

### Expected Output
```
Top 2 videos in genre 'Business': Investing 101 Crash Course (4.8), Ultimate Guide to Digital Marketing (4.5)
Top 2 videos in genre 'Tech': Rust Crash Course (4.9), React Crash Course (4.7)
Top 2 videos in genre 'Lifestyle': Weekly Yoga Routine for Beginners (4.3), 10-Minute Meditation (4.1)
Top 2 videos in genre 'Comedy': Standup Comedy Special (4.9), Funny Animal Fails Compilation (4.6)
```

### Notes
- Two common approaches: full sort per group, or heap of size 2 per group.
- Handle titles present in only one dataset.
- Variant: top N, top by other metric (watch_duration_mins).

---

## Q3: Queue Buffer Processing (Newsfeed Engagement)
**Source:** FS2 Newsfeed — Q1 (meta_de_prep_code.md)
**Pattern:** Sliding window / fixed-size FIFO buffer with filters
**Difficulty:** Medium
**Time target:** 20 mins

### Problem Statement
A queue is a buffer/cache with a fixed size. Assuming the buffer holds only 3 events, write a function that processes an event stream. Every time the buffer is full, output:
1. The number of engagements,
2. The total time spent (in seconds, from `viewed_time_ms`),
3. The post IDs in the buffer.

Ignore any test content (items where `is_test_content` is True).

### Input
```python
stream = [
    {'post_id': 101, 'viewed_time_ms': 6500, 'engaged_with_post': 1},
    {'post_id': 104, 'viewed_time_ms': 200,  'engaged_with_post': 1},
    {'post_id': 105, 'viewed_time_ms': 4200, 'engaged_with_post': 0, 'is_test_content': True},
    {'post_id': 108, 'viewed_time_ms': 4499, 'engaged_with_post': 1},
    {'post_id': 105, 'viewed_time_ms': 500,  'engaged_with_post': 1},
]
```

### Expected Output
```
You've got 2 engagements and spent 6.700s viewing content. Post ids: 101, 104
You've got 2 engagements and spent 4.699s viewing content. Post ids: 104, 108
You've got 2 engagements and spent 4.999s viewing content. Post ids: 108, 105
```

### Notes
- Buffer size = 3 in the example, but the test case shows 2-item buffers in the output (the third print is after the 5th non-test event). Confirm whether "full" means exactly N events held, or a sliding window of the last N (the expected output is a sliding window of 2 — clarify with interviewer).
- Must skip `is_test_content=True`.
- Convert ms to seconds with 3 decimals.
- Follow-up: what if buffer size or filter rules change dynamically?

---

## Q4: Delivery Time Estimation (Routing State Machine)
**Source:** FS4 Instamart — Q1 (meta_de_prep_code.md)
**Pattern:** Per-driver state machine + travel_time matrix lookups, running clock
**Difficulty:** Hard
**Time target:** 25 mins

### Problem Statement
Given a list of `routing_steps` (each step is TRAVEL, PICKUP, or DROPOFF for a given driver) and a `travel_time` matrix indexed by `location_id`, create a mapping from `order_id` to the expected delivery time.

### Input
```python
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
    {"driver_id": 2, "action": "DROPOFF", "order_id": 3},
]

travel_time = [
    [ 0, 10, 17, 23,  9, 13 ],
    [10,  0, 30, 25,  5, 25 ],
    [17, 30,  0, 10,  7, 33 ],
    [23, 25, 10,  0, 11, 35 ],
    [ 9,  5,  7, 11,  0, 27 ],
    [13, 25, 33, 35, 27,  0 ],
]
```

### Expected Output
Print in the format:
```
order <order_id> is expected to be delivered at <time>
```
for each order, where `<time>` is accumulated travel time per driver.

### Notes
- Must track per-driver current location and cumulative time.
- `travel_time[i][j]` is the cost to go from location i to location j.
- First TRAVEL for a driver: decide whether it costs anything (likely starts at location 0 or costs 0 to arrive at the first declared location — clarify with interviewer).
- PICKUP/DROPOFF actions happen at the driver's current location (post-TRAVEL).

---

## Q5: Vehicle Capacity Feasibility (Carpool Sweep)
**Source:** FS3 Carpool — Q1 (meta_de_prep_code.md)
**Pattern:** Sweep-line / timeline event processing with capacity constraint
**Difficulty:** Medium
**Time target:** 15 mins

### Problem Statement
Given the total passenger capacity of a vehicle and a list of booking requests (with pickup/drop-off times and passenger counts), return `True` if all bookings can be accommodated without exceeding capacity at any point in time; otherwise `False`.

### Input
```python
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
    {"request_id": 9, "pickup_time": 90, "drop_off_time": 100, "total_passengers": 1},
]
```

### Expected Output
```
True
```

### Notes
- Classic sweep-line: sort events (pickup=+passengers, drop-off=-passengers), handle drop-offs before pickups at the same timestamp.
- Follow-up: return the list of bookings that caused overflow, or maximum concurrent passenger count.

---

## Q6: Reciprocal Followers + Follow Recommendations (Twitter Graph)
**Source:** FS5 Twitter — Q1 (meta_de_prep_code.md)
**Pattern:** Graph/set intersection + recommendation by second-degree connections
**Difficulty:** Medium
**Time target:** 20 mins

### Problem Statement
For each user, return a list `[reciprocal_follower_count, follow_recommendations]` where:
- A reciprocal follower is a user that the current user follows AND who follows them back.
- A follow recommendation is any user in the overall user set that is not the user themselves and not already followed by the user.

### Input
```python
user_follows = {
    'A': ['B', 'C', 'D'],
    'B': ['D', 'E', 'F', 'A'],
    'C': ['F', 'A', 'E'],
    'D': [],
}
```

### Expected Output
```python
{
    'A': [2, ['E', 'F']],
    'B': [1, ['C']],
    'C': [1, ['B', 'D']],
    'D': [0, ['A', 'B', 'C', 'E', 'F']],
}
```

### Notes
- Universe of users = union of keys + all values (E and F appear only as followees).
- Recommendations should exclude self and already-followed users (ordering likely sorted).
- Follow-up: friends-of-friends recommendation, ranking by count of mutual friends.

---

## Q7: Single Car Rental Conflict (Interval Acceptance)
**Source:** FS6 Car Rental — Q1 (meta_de_prep_code.md)
**Pattern:** Greedy interval acceptance vs. conflict with accepted intervals
**Difficulty:** Medium
**Time target:** 15 mins

### Problem Statement
Given an ordered dictionary of rental orders (by `order_id`) for a single car, accept or reject each order based on conflict with previously accepted orders. Process in `order_id` order.

### Input
```python
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
```

### Expected Output
```
21 ok, 22 ok, 23 rejected, 24 rejected, 25 ok, 26 ok, 27 rejected, 28 rejected, 29 ok
```

### Notes
- Clarify whether same-day pickup/dropoff counts as conflict (touching intervals).
- Variant: multiple cars / pool — assign greedily or return count of cars needed.
- Data structure choice: interval list vs. sorted structure for fast conflict check.

---

## Q8: Streaming Genre Running Average (YouTube Variant)
**Source:** YouTube Artifact — Python Q1 (meta_de_prep_code.md)
**Pattern:** Running average per genre, emitted after each event
**Difficulty:** Easy
**Time target:** 10 mins

### Problem Statement
Process `youtube_video_views` row by row. After each row, print the genre and its current running average `watch_duration_mins` across all rows seen so far in that genre.

### Input
```python
youtube_video_views = [
    {"title": "Ultimate Guide to Digital Marketing", "video_length_mins": 50, "genre": "Business",   "watch_duration_mins": 45},
    {"title": "Investing 101 Crash Course",          "video_length_mins": 70, "genre": "Business",   "watch_duration_mins": 50},
    {"title": "React Crash Course",                  "video_length_mins": 60, "genre": "Technology", "watch_duration_mins": 55},
    {"title": "Rust Crash Course",                   "video_length_mins": 60, "genre": "Technology", "watch_duration_mins": 50},
    {"title": "Weekly Yoga Routine for Beginners",   "video_length_mins": 25, "genre": "Lifestyle",  "watch_duration_mins": 20},
    {"title": "10-Minute Meditation for Stress Relief","video_length_mins": 10, "genre": "Lifestyle","watch_duration_mins": 15},
    {"title": "Standup Comedy Special",              "video_length_mins": 60, "genre": "Comedy",     "watch_duration_mins": 15},
    {"title": "Funny Animal Fails Compilation",      "video_length_mins": 15, "genre": "Comedy",     "watch_duration_mins": 15},
    {"title": "How to Make Your Friends Laugh",      "video_length_mins": 6,  "genre": "Comedy",     "watch_duration_mins": 6},
]
```

### Expected Output
After each row, print the genre name and the running average `watch_duration_mins` for that genre.

### Notes
- Maintain `{genre: (sum, count)}` — update in O(1) per event.
- Related to Q1 but streaming-print semantics differ (output every row).

---

## Q9: Most Common Theme Across Books
**Source:** Meta Screen Set 1 — Python Q2
**Pattern:** Counter/dictionary tally over nested lists
**Difficulty:** Easy
**Time target:** 10 mins

### Problem Statement
Given a mapping between book titles and a list of themes they contain, find the theme that appears the most times across all books. If multiple themes tie, return any one of them.

### Input
Signature: `most_common_theme(book_themes: Dict[str, List[str]]) -> Optional[str]`

Example input:
```python
{
    "1984":            ["dystopia", "politics"],
    "Brave New World": ["dystopia", "science"],
    "Fahrenheit 451":  ["dystopia"],
}
```

### Expected Output
`"dystopia"`

### Notes
- Edge cases: empty mapping, empty theme lists, `None` input, case-sensitive equality ("Love" vs "love").
- O(n) time, O(k) space (k = distinct themes).

---

## Q10: Most Common Unique Comment Across Locations
**Source:** Meta Screen Set 2 — Python Q2
**Pattern:** De-duplicate per group, then count across groups
**Difficulty:** Easy
**Time target:** 10 mins

### Problem Statement
Given a dictionary mapping locations to lists of comments, return the most common comment across all locations, where duplicates within a single location are collapsed (counted once per location).

### Input
```python
comments = {
    "Blue Hill":     ["Cozy Atmosphere", "Curated Selection", "Too busy", "Cozy Atmosphere"],
    "Apple Road":    ["Good Parking", "Sweet and Delicious", "Perfect for rainy Days"],
    "Orange Avenue": ["Cozy Atmosphere", "Large Bike Storage", "Curated Selection", "Curated Selection"],
}
```

### Expected Output
`"Cozy Atmosphere"` or `"Curated Selection"` (both appear at 2 locations — return either).

### Notes
- Use `set(comments[location])` to get unique-per-location first.
- Related to Q9 but with per-group deduplication twist.
- Handle `None`, empty dict, empty lists.

---

## Q11: Peak Concurrent Meeting Attendance
**Source:** Meta Screen Set 1 — Python Q3
**Pattern:** Hour bucket tally / sweep line on discrete time
**Difficulty:** Medium
**Time target:** 15 mins

### Problem Statement
Given a list of meetings (each with start hour, end hour as integers 1–24, and audience size), return the largest total number of people attending meetings simultaneously across any continuous period of time.

### Input
Signature: `largest_number_of_people(meetings: List[Meeting]) -> int`
Each `Meeting` has `.start`, `.end`, `.audience`.

### Expected Output
An integer: peak concurrent attendance across all hours.

### Notes
- Either bucket-tally each hour (O(H·N)) or use sweep-line events (O(N log N)).
- Clarify whether `end` hour is inclusive or exclusive.

---

## Q12: Largest Classes Across 2 Consecutive Years (Workshops)
**Source:** Meta Screen Set 2 — Python Q3
**Pattern:** Year bucket tally + max over consecutive pair window
**Difficulty:** Medium
**Time target:** 15 mins

### Problem Statement
The bookstore hosts yearly workshops; each workshop repeats the same classes per year between `start` and `end` (inclusive). Given a list of workshops, return the largest number of classes hosted in total across any 2 consecutive years.

### Input
```python
[
    Workshop(name="Intro to archival science", classes_per_year=4,  start=2000, end=2003),
    Workshop(name="Bind your own book",         classes_per_year=1,  start=2001, end=2004),
    Workshop(name="Monthly discussion circle",  classes_per_year=12, start=2004, end=2006),
]
```

### Expected Output
`25` — the pair 2004 (13 classes) + 2005 (12 classes).

### Notes
- Related to Q11 (same sweep pattern, different domain).
- Fill missing years with 0 between min and max year, then slide a window of size 2.

---

## Q13: Uncommon Elements Between Two Lists
**Source:** Meta Screen Set 2 — Python Q4
**Pattern:** Symmetric difference, order-preserving, de-duplicated
**Difficulty:** Easy
**Time target:** 7 mins

### Problem Statement
Given two lists, return elements that appear in exactly one of the two lists (symmetric difference), with no duplicates. Order should be preserved (first list's unique items first, then second list's).

### Input / Expected Output
```python
find_uncommon_elements([], [])                     == []
find_uncommon_elements([1, 2, 3], [4, 5, 6])        == [1, 2, 3, 4, 5, 6]
find_uncommon_elements([1, 2, 2, 3], [3, 3, 4])     == [1, 2, 4]
find_uncommon_elements([1, 2, 3], [1, 2, 3])        == []
find_uncommon_elements([1, -2, 3], [-1, 2, 3])      == [1, -2, -1]
find_uncommon_elements(["apple", "banana"], ["banana", "cherry"]) == ["apple", "cherry"]
find_uncommon_elements([10, 20, 30], [30, 40, 50])  == [10, 20, 40, 50]
```

### Notes
- Sets alone won't preserve order; combine set-based filtering with a seen-tracker while iterating.

---

## Q14: Find Editions of a Book (Prefix Match)
**Source:** Meta Screen Set 3 — Python Q2 (Additional)
**Pattern:** Sort + two-pointer prefix grouping (or O(n^2) naive)
**Difficulty:** Medium
**Time target:** 15 mins

### Problem Statement
Given a list of book titles, find all "editions" of the same original book. An edition is any title that has the full title of another book (the original) as a prefix followed by a space (so the original title is a full word-prefix).

### Input / Expected Output
```python
find_editions(["Green Trees", "Python in a Nutshell"]) == []
sorted(find_editions(["Green Trees", "Green Trees Second Edition",
                      "Python in a Nutshell", "Python for Dummies", "Python"])) \
    == sorted(['Python in a Nutshell', 'Python for Dummies', 'Green Trees Second Edition'])
find_editions(["Python", "All About Pythons"]) == []
```

### Notes
- "All About Pythons" is not an edition of "Python" — the prefix must be followed by a space (or be a full word boundary).
- Two-pointer after sort: O(n log n); naive double loop: O(n^2).

---

## Q15: Bookshelf Fitting (Titles by Length)
**Source:** Meta Screen Set 3 — Python Q4 (Additional)
**Pattern:** Greedy match longest title to largest shelf
**Difficulty:** Medium
**Time target:** 10 mins

### Problem Statement
Given two lists `bookshelf_sizes` (ints) and `bookshelf_titles` (strings), determine whether every title fits into some unused shelf, where a title fits into a shelf iff `len(title) <= size`. Each shelf holds at most one title.

### Input / Expected Output
```python
can_fit_books([7, 9, 6],       ['education', 'novel', 'fiction'])   == True
can_fit_books([7, 9, 4],       ['education', 'novel', 'fiction'])   == False
can_fit_books([6, 4],          ['fantasy', 'science', 'romance'])   == False   # more titles than shelves
can_fit_books([7, 8, 9, 5],    ['mystery', 'classic'])              == True
can_fit_books([10],            ['encyclopedia'])                    == False
can_fit_books([4],             ['note'])                            == True
can_fit_books([3, 4, 5],       ['a', 'bc', 'def'])                  == True
can_fit_books([2, 2],          ['aa', 'bbb'])                       == False
can_fit_books([3, 4, 5],       [])                                  == True
can_fit_books([],              ['book'])                            == False
```

### Notes
- Sort titles descending by length, shelves descending by size, check pairwise.
- Edge cases: empty titles, empty shelves, more titles than shelves.

---

## Q16: Nth Most Sold Book (Two Tie-Break Variants)
**Source:** Meta Screen Set 3 — Python Q4 Version 2 (Additional)
**Pattern:** Multi-key sort, 1-indexed rank
**Difficulty:** Easy
**Time target:** 10 mins

### Problem Statement
Given a list of `(price, sales_count)` tuples and an integer `n` (1-indexed), return the nth most sold book as a tuple. Primary sort is descending by `sales_count`. Secondary (tie-breaker) comes in two variants:
- **Version 1:** ascending by price (cheaper book wins tie).
- **Version 2:** descending by price (more expensive book wins tie).

Return `None` if the list is empty or `n` is invalid (`n <= 0` or `n > len`).

### Input
```python
books = [
    (20.99, 5), (15.99, 7), (30.50, 5), (10.99, 3), (25.00, 7),
]
```

### Expected Output
- Version 1, n=1 → `(15.99, 7)`
- Version 2, n=1 → `(25.00, 7)`

### Notes
- Use `sorted(..., key=lambda x: (-x[1], x[0]))` for V1; `sorted(..., key=lambda x: (x[1], x[0]), reverse=True)` for V2.
- Edge cases: empty list, n = 0, n too large.

---

## Q17: Max Reading-Program Score (3 Distinct Categories)
**Source:** Meta Screen Set 4 — Python Q1
**Pattern:** Group-by + top-1 per group + sum top-3
**Difficulty:** Medium
**Time target:** 12 mins

### Problem Statement
A student can score points from up to 3 books, each from a different category. Given a list of `(category, score)` tuples for books the student read, return the maximum possible total score.

### Input / Expected Output
```python
get_max_score([("Adventure",5), ("Adventure",2), ("History",3)])            == 8
get_max_score([("Adventure",4), ("History",3), ("Reference",1), ("Fiction",2)]) == 9
get_max_score([("Biography",2), ("Biography",4), ("Science",3), ("Science",1)]) == 7
get_max_score([])                                                            == 0
```

### Notes
- Keep only the best score per category, then sum the top 3.
- Edge cases: fewer than 3 categories, empty list.

---

## Q18: Validate Library Checkout/Return Log
**Source:** Meta Screen Set 4 — Python Q2
**Pattern:** Set-based state tracking over ordered events
**Difficulty:** Easy
**Time target:** 10 mins

### Problem Statement
Each log entry has a `book_id` and `is_checkout` flag (True = checkout, False = return). Given an ordered list of entries, return `False` if:
- a book is checked out while already checked out, or
- a book is returned while not currently checked out.

Otherwise return `True`. Assume no books are checked out before the first log entry.

### Input / Expected Output
```python
[LogEntry(1, T), LogEntry(2, T), LogEntry(1, F), LogEntry(1, T)]    # True
[LogEntry(1, T), LogEntry(2, T), LogEntry(1, T)]                     # False
[LogEntry(1, T), LogEntry(2, F), LogEntry(1, T)]                     # False
[LogEntry(1, T), LogEntry(2, T), LogEntry(1, F), LogEntry(2, F)]    # True
[LogEntry(3, T), LogEntry(3, F), LogEntry(3, F)]                     # False
```

### Notes
- Use a set of currently-checked-out book IDs.
- Streaming pattern: similar to queue-buffer (Q3).

---

## Q19: Library Location Reroute on Closure
**Source:** Meta Screen Set 4 — Python Q3
**Pattern:** Nested-dict aggregation with set-based filtering
**Difficulty:** Medium
**Time target:** 15 mins

### Problem Statement
The library has 4 locations. The given dictionary shows, for each location, how many employees are normally assigned there, and how many will go to each other location (or home) when theirs closes. Given a set of closed locations, return a dict of the total number of employees expected at each remaining (open) location.

### Input
```python
location_employees = {
    "A": {"A": 80,  "B": 10,  "C": 15,  "D": 5},
    "B": {"A": 5,   "B": 120, "C": 10,  "D": 0},
    "C": {"A": 5,   "B": 10,  "C": 110, "D": 0},
    "D": {"A": 5,   "B": 0,   "C": 10,  "D": 240},
}
```

### Expected Output
```python
get_num_employees(loc, set())              == {"A": 80,  "B": 120, "C": 110, "D": 240}
get_num_employees(loc, {"A"})              == {"B": 130, "C": 125, "D": 245}
get_num_employees(loc, {"D", "C"})         == {"A": 90,  "B": 130}
get_num_employees(loc, {"A","B","C","D"})  == {}
```

### Notes
- Note that the "home" rerouting accounts for the total on the diagonal being less than the full assigned count — read input dict carefully.
- Returned dict should exclude closed locations.

---

## Q20: Deletions to Transform src → tgt (Two Variants)
**Source:** Meta Screen Set 4 — Python Q4
**Pattern:** Two-pointer subsequence check + counting
**Difficulty:** Medium
**Time target:** 15 mins

### Problem Statement
Given two words `src` and `tgt`, determine whether `tgt` can be formed by deleting characters from `src` without re-ordering. If impossible, return `None`.

Two variants:
- **Total count** of deleted characters (counting duplicates).
- **Distinct letters** deleted (set cardinality of removed letters).

### Input / Expected Output
Total variant:
```python
deletions_total("fiction", "fin") == 4
deletions_total("balloon", "ban") == 4   # l,l,o,o
deletions_total("abc", "abc")     == 0
deletions_total("abc", "")        == 3
deletions_total("abc", "abcd")    is None
deletions_total("abc", "cba")     is None
deletions_total("", "a")          is None
deletions_total(None, "a")        is None
```

Distinct variant:
```python
deletions_distinct("abc", "abc")     == 0
deletions_distinct("balloon", "ban") == 2   # {l, o}
deletions_distinct("abc", "")        == 3
deletions_distinct("abc", "abcd")    is None
deletions_distinct("abc", "cba")     is None
deletions_distinct("", "a")          is None
deletions_distinct("abc", None)      is None
```

### Notes
- Classic subsequence two-pointer with counting twist.
- Don't forget to drain the tail of `src` once `tgt` is matched.
- `None` inputs → return `None`.
