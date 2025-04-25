
let print_date (tm:Unix.tm):string =
  Printf.sprintf "%04d-%02d-%02d" (tm.tm_year + 1900) (tm.tm_mon + 1) tm.tm_mday

let random_int ~(min:int) ~(max:int):int =
  let range = max - min + 1 in
  Random.int range + min

let gaussian ~mu ~sigma =
  let rec sample () =
    let u1 = Random.float 1.0 in
    let u2 = Random.float 1.0 in
    let z0 = sqrt (-2.0 *. log u1) *. cos (2.0 *. Float.pi *. u2) in
    let result = mu +. sigma *. z0 in
    if result < 0.0 || result > 100.0 then sample () else result
  in
  sample

let random_date ~(start:Unix.tm) ~(end_:Unix.tm) =
  let start_time = Unix.mktime start |> fst |> int_of_float in
  let end_time = Unix.mktime end_ |> fst |> int_of_float in
  let seconds_per_day = 86400 in
  let diff_in_days = (end_time - start_time) / seconds_per_day in
  
  let random_offset = (Random.int diff_in_days) * seconds_per_day in
  Printf.printf "\nRandom offset in days: %d\n" random_offset;
  let random_timestamp = float_of_int (start_time + random_offset) in
  Printf.printf "Random timestamp: %s\n" (print_date (Unix.gmtime random_timestamp));
  Unix.gmtime random_timestamp

let years_delta (start:Unix.tm)(end_:Unix.tm):int =
  let years = end_.tm_year - start.tm_year in
  if end_.tm_mon < start.tm_mon || (end_.tm_mon = start.tm_mon && end_.tm_mday < start.tm_mday) then
    years - 1
  else
    years

let advance_date (tm:Unix.tm) (years:int):Unix.tm =
  let new_time = {tm with Unix.tm_year = tm.tm_year + years} in
  let _, corrected_tm = Unix.mktime new_time in
  corrected_tm

