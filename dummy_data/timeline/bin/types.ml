

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

type event = {
  recorded_on_day_after_referral:int;
  name: string;
}

type observation = {
  recorded_on_day_after_referral:int;
  name: string;
  value_as_string: string;
}

type patient = {
  id: int;
  sex: sex;
  date_of_birth: Unix.tm;
  events : event list;
  observations : observation list;
}

type statistics_key = {
  label: string;
  mean: string;
  std: string;
}




