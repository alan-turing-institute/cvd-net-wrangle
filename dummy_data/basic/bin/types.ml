

type sex = Male | Female | Missing
let count_sexes = List.length [Male;Female]
let sex_of_index (i:int) : sex = 
match i with
  | 0 -> Male
  | 1 -> Female
  | _ -> Missing

let string_of_sex (s:sex):string=
match s with
| Male -> "Male"
| Female -> "Female"
| Missing -> "Missing"

type patient = {
  id: int;
  sex: sex;
  date_of_birth: Unix.tm;
  date_of_referral: Unix.tm;
  (* weight: float; *)
}

