(* open Types *)


(** [print_date tm] formats a [Unix.tm] record into a string representation 
    in the format "YYYY-MM-DD".

    @param tm The date to format as a [Unix.tm] record.
    @return A string representing the formatted date.
*)
let print_date (tm:Unix.tm):string =
  Printf.sprintf "%04d-%02d-%02d" (tm.tm_year + 1900) (tm.tm_mon + 1) tm.tm_mday

(** [random_int ~min ~max] generates a random integer within the inclusive range 
    between [min] and [max].

    @param min The minimum value of the range.
    @param max The maximum value of the range.
    @return A random integer within the specified range.
*)

let generate_id (count:int):int=
  count+1
let generate_date ~(years_since_1900:int) : Unix.tm = 
  {
      Unix.tm_sec = 0;
      tm_min = 0;
      tm_hour = 0;
      tm_mday = 1;
      tm_mon = 0;    (* January = 0 *)
      tm_year = years_since_1900; (* 2000 - 1900 = 100 *)
      tm_wday = 0; tm_yday = 0; tm_isdst = false
  } 

let random_int ~(min:int) ~(max:int):int =
  let range = max - min + 1 in
  Random.int range + min

let random_float ~(min:float) ~(max:float):float =
  let range = max -. min +. 1. in
  Random.float range +. min

(** [gaussian ~mu ~sigma] generates a random number following a Gaussian 
    (normal) distribution with mean [mu] and standard deviation [sigma]. 
    The result is constrained to the range [0.0, 100.0].

    @param mu The mean of the distribution.
    @param sigma The standard deviation of the distribution.
    @return A random number following the specified Gaussian distribution.
*)
let gaussian ~(mu:float) ~(sigma:float):float =
  let u1 = Random.float 1.0 in
  let u2 = Random.float 1.0 in
  let z0 = sqrt (-2.0 *. log u1) *. cos (2.0 *. Float.pi *. u2) in
  mu +. sigma *. z0 

(** [random_date ~start ~end_] generates a random date between the given 
    [start] and [end_] dates.

    @param start The starting date as a [Unix.tm] record.
    @param end_ The ending date as a [Unix.tm] record.
    @return A random date as a [Unix.tm] record within the specified range.
*)
let random_date ~(start:Unix.tm) ~(end_:Unix.tm) =
  let start_time = Unix.mktime start |> fst |> int_of_float in
  let end_time = Unix.mktime end_ |> fst |> int_of_float in
  let seconds_per_day = 86400 in
  let diff_in_days = (end_time - start_time) / seconds_per_day in
  
  let random_offset = (Random.int diff_in_days) * seconds_per_day in
  (* Printf.printf "\nRandom offset in days: %d\n" random_offset; *)
  let random_timestamp = float_of_int (start_time + random_offset) in
  (* Printf.printf "Random timestamp: %s\n" (print_date (Unix.gmtime random_timestamp)); *)
  Unix.gmtime random_timestamp

(** [years_delta start end_] calculates the difference in years between 
    two dates, accounting for whether the end date has passed the 
    anniversary of the start date.

    @param start The starting date as a [Unix.tm] record.
    @param end_ The ending date as a [Unix.tm] record.
    @return The number of full years between the two dates.
*)  
let years_delta (start:Unix.tm)(end_:Unix.tm):int =
  let years = end_.tm_year - start.tm_year in
  if end_.tm_mon < start.tm_mon || (end_.tm_mon = start.tm_mon && end_.tm_mday < start.tm_mday) then
    years - 1
  else
    years

(** [advance_date start years] takes a date represented as a [Unix.tm] record 
    and advances it by the specified number of years. It adjusts the resulting 
    date to account for edge cases such as leap years or invalid dates.

    @param start The starting date as a [Unix.tm] record.
    @param years The number of years to advance the date by.
    @return A [Unix.tm] record representing the adjusted date.
*)
let advance_date (start:Unix.tm) (years:int):Unix.tm =
  let new_time = {start with Unix.tm_year = start.tm_year + years} in
  let _, corrected_tm = Unix.mktime new_time in
  corrected_tm

(** [next_date start years] calculates the next date by advancing the given 
    [start] date by the specified number of years. If the resulting date is 
    in the future, it generates a random date between the start date and 
    the current date.

    @param start The starting date as a [Unix.tm] record.
    @param years The number of years to advance the date by.
    @return A [Unix.tm] record representing the next date.
*)
let next_date (start:Unix.tm) (years:int): Unix.tm =
  let new_date, _ = Unix.mktime @@ advance_date start years in
  let current_date = Unix.time () in
  
  if new_date > current_date then
    random_date ~start:start ~end_:(Unix.gmtime current_date)
  else
    Unix.gmtime new_date


let load_yaml (file_path:Fpath.t):Yaml.value =
  match Yaml_unix.of_file file_path with
  | Ok yaml -> yaml
  | Error (`Msg e) -> failwith ("Failed to read YAML file" ^ e)
    
let yaml_to_hashtbl (file_path : Fpath.t) : (string, int) Hashtbl.t =
  let dict = Hashtbl.create 10 in
  match Yaml_unix.of_file file_path with
  | Ok (`O assoc) ->
      List.iter (fun (key, value) ->
        let int_value =
          match value with
          | `Float f -> int_of_float f
          | `String s -> int_of_string s
          | _ -> failwith ("Unsupported value for key: " ^ key)
        in Hashtbl.add dict key int_value
        ) assoc;
      dict
  | Ok _ -> failwith "Expected a top-level YAML mapping"
  | Error (`Msg msg) -> failwith ("YAML parse error: " ^ msg)
