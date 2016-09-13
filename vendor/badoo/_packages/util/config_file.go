package util

// XXX(antoxa): for the lack of a better spot to put this code - let it rot here
// the point being - moved the code here from service package, since some utils need it
// but utils can't easily use service package itself, due to a ton of init machinery there

import (
	"badoo/_packages/util/structs"

	"encoding/json"
	"io/ioutil"
)

func ParseConfigFromFile(conf_path string, to_struct interface{}) error {
	data, err := ioutil.ReadFile(conf_path)
	if err != nil {
		return err
	}

	data = PrepareJsonConfigData(data)

	err = ParseConfigToStruct(data, to_struct)
	if err != nil {
		return err
	}

	return nil
}

func ParseConfigToStruct(data []byte, to_struct interface{}) error {

	err := json.Unmarshal(data, to_struct)
	if err != nil {
		return err
	}

	err = structs.PBCheckRequiredFields(to_struct)
	if nil != err {
		return err
	}

	return nil
}

func PrepareJsonConfigData(d []byte) []byte {
	data := make([]byte, len(d))
	copy(data, d)

	l := len(data)

	// first pass, remove comments
	for i := 0; i < l; {
		switch data[i] {
		default:
			i++
		case '"': // no substitutions inside quoted strings
			i++
			for ; i < l; i++ {
				if '"' == data[i] && '\\' != data[i-1] {
					i++
					break
				}
			}
		case '/':
			i = jsonDataConsumeComments(data, i+1)
		}
	}

	// second pass, remove trailing commas
	for i := 0; i < l; i++ {
		if ']' == data[i] || '}' == data[i] {
			j := i - 1

			for j >= 0 && jsonIsWhitespace(data[j]) {
				j--
			}

			for ; ',' == data[j]; j-- {
				data[j] = ' '
			}
		}
	}

	return data
}

// ----------------------------------------------------------------------------------------------------------------------------------
// private json stuff

func jsonDataConsumeComments(data []byte, i int) int {
	l := len(data)
	if i >= l {
		return i
	}

	if '/' == data[i] { // line comment
		data[i-1] = ' '
		for ; (i < l) && ('\n' != data[i]); i++ {
			data[i] = ' '
		}
	} else if '*' == data[i] { // block comment
		data[i-1] = ' '
		data[i] = ' '

		for i++; i < l; i++ {
			if '*' == data[i] {
				if ((i + 1) < l) && '/' == data[i+1] {
					data[i] = ' '
					data[i+1] = ' '
					i += 2
					break
				}
			}
			data[i] = ' '
		}
	}
	return i
}

func jsonIsWhitespace(c byte) bool {
	return ' ' == c || '\t' == c || '\r' == c || '\n' == c
}
