package router

import (
	"net/url"

	"github.com/juju/errgo"
	"gopkg.in/juju/charm.v3"
)

// A FieldQueryFunc is used to retrieve a metadata document for the given URL,
// selecting only those fields specified in keys of the given selector.
type FieldQueryFunc func(id *charm.Reference, selector map[string]int) (interface{}, error)

// A FieldUpdateFunc is used to update a metadata document for the
// given URL. For each field in updateOp, it will set that field to
// its corresponding value in the metadata document.
type FieldUpdateFunc func(id *charm.Reference, updateOp map[string]interface{}) error


// A FieldHandlerFunc returns some data from the given document. The
// document will have been returned from an earlier call to the
// associated QueryFunc.
type FieldGetFunc func(doc interface{}, id *charm.Reference, path string, method string, flags url.Values) (interface{}, error)

// FieldPutFunc sets values in updateOp corresponding to fields to be set
// in the metadata document for the given URL. The path holds the metadata path
// after the initial prefix has been removed.
type FieldPutFunc func(id *charm.Reference, path string, updateOp map[string] interface{}) error

// FieldIncludeHandlerParams specifies the parameters for NewFieldIncludeHandler.
type FieldIncludeHandlerParams struct {
	// Key is used to group together similar FieldIncludeHandlers
	// (the same query should be generated for any given key).
	Key    interface{}
	
	// Query is used to retrieve the document from the database
	// The fields passed to the query will be the union of all fields found
	// in all the handlers in the bulk request.
	Query  FieldQueryFunc

	// Update is used to update the document in the database.
	Update FieldUpdateFunc

	// Fields specifies which fields are required by the given handler.
	Fields []string

	// Handle actually retrieves the data from the document.
	HandleGet FieldGetFunc

	// HandlePut generates update operations for a PUT
	// operation.
	HandlePut FieldPutFunc
}

type fieldIncludeHandler struct {
	p FieldIncludeHandlerParams
}

// FieldIncludeHandler returns a BulkIncludeHandler that will perform
// only a single database query for several requests. See FieldIncludeHandlerParams
// for more detail.
//
// See in ../v4/api.go for an example of its use.
func FieldIncludeHandler(p FieldIncludeHandlerParams) BulkIncludeHandler {
	return &fieldIncludeHandler{p}
}

func (h *fieldIncludeHandler) Key() interface{} {
	return h.p.Key
}

func (h *fieldIncludeHandler) HandlePut(hs []BulkIncludeHandler, id *charm.Reference, paths []string, values []*json.RawMessage) []error {
	funcs := make([]FieldPutFunc, len(hs))
	// Extract the handler functions.
	for i, h := range hs {
		h := h.(*fieldIncludeHandler)
		funcs[i] = h.p.HandlePut
	}
	updateOp := make(map[string]interface{})
	for _, f := range funcs {
		if err := f(id, path, updateOp); err 
	}
	if err := h.Update(id, updateOp)
}

func (h *fieldIncludeHandler) HandleGet(hs []BulkIncludeHandler, id *charm.Reference, paths []string, method string, flags url.Values) ([]interface{}, error) {
	funcs := make([]FieldHandlerFunc, len(hs))
	selector := make(map[string]int)
	// Extract the handler functions and union all the fields.
	for i, h := range hs {
		h := h.(*fieldIncludeHandler)
		funcs[i] = h.p.HandleGet
		for _, field := range h.p.Fields {
			selector[field] = 1
		}
	}
	// Make the single query.
	doc, err := h.p.Query(id, selector)
	if err != nil {
		// Note: preserve error cause from handlers.
		return nil, errgo.Mask(err, errgo.Any)
	}

	// Call all the handlers with the resulting query document.
	results := make([]interface{}, len(hs))
	for i, f := range funcs {
		var err error
		results[i], err = f(doc, id, paths[i], method, flags)
		if err != nil {
			// TODO correlate error with handler (perhaps return
			// an error that identifies the slice position of the handler that
			// failed).
			// Note: preserve error cause from handlers.
			return nil, errgo.Mask(err, errgo.Any)
		}
	}
	return results, nil
}
