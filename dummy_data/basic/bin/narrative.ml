open Types
open Utilities

(* This module generates dummy patient data for testing purposes. *)

(* The patient record type *)
let get_date_of_referral (p:patient) : Unix.tm = 
  let offset = (Random.int 25) + 100 in
  let start_date = {
  Unix.tm_sec = 0;
  tm_min = 0;
  tm_hour = 0;
  tm_mday = 1;
  tm_mon = 0;    (* January = 0 *)
  tm_year = offset; (* 2000 - 1900 = 100 *)
  tm_wday = 0; tm_yday = 0; tm_isdst = false
} in

let end_date = {
  Unix.tm_sec = 0;
  tm_min = 0;
  tm_hour = 0;
  tm_mday = 1;
  tm_mon = 0;
  tm_year = 125;  (* 2025 - 1900 *)
  tm_wday = 0; tm_yday = 0; tm_isdst = false
} in

random_date ~start:start_date ~end_:end_date
