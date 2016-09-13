// reflection based tools for checking/processing config (and maybe not just config) data
package structs

import (
	"reflect"
)

// walks the gpb message and calls function `f` for each struct field
// handles pointer/slice indirections
// TODO(antoxa): move this to badoo/_packages/util somewhere
func WalkStructFieldsRecursive(rv reflect.Value, f func(parent reflect.Value, fieldNo int) error) (err error) {

	rt := rv.Type()

	switch rt.Kind() {
	case reflect.Struct:

		// handle all struct fields recursively, depth first
		for i := 0; i < rt.NumField(); i++ {

			// user function first
			err := f(rv, i)
			if err != nil {
				return err
			}

			// recurse second
			err = WalkStructFieldsRecursive(rv.Field(i), f)
			if nil != err {
				return err
			}
		}

	case reflect.Slice:
		// handle all slice values recursively, depth first
		for i := 0; i < rv.Len(); i++ {
			err = WalkStructFieldsRecursive(rv.Index(i), f)
			if nil != err {
				return err
			}
		}

	case reflect.Ptr:
		// just recurse into pointers, this is required for struct pointers
		//  recursion will terminate inside this call if it's just a pointer struct field
		if !rv.IsNil() {
			return WalkStructFieldsRecursive(reflect.Indirect(rv), f)
		}
	}

	return nil
}
