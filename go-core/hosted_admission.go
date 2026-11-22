package main

import (
	"context"
	"errors"
	"time"
)

type hostedControlHTTPError struct{ status int }

func (*hostedControlHTTPError) Error() string { return "hosted control rejected request" }

// Capture the caller's authority/configuration before admission. Compensation
// must not bind itself to a replacement desktop session or mutable caller headers.
type hostedAdmissionCaller struct {
	client             core
	authorization, key string
	abortBody          map[string]any
}

func newHostedAdmissionCaller(c *core, rctx context.Context, authorization, key string, params map[string]any) *hostedAdmissionCaller {
	a := &hostedAdmissionCaller{client: core{control: c.control, publicStateless: c.publicStateless, publicInternalToken: c.publicInternalToken}, authorization: authorization, key: key, abortBody: make(map[string]any)}
	for _, field := range []string{"attemptId", "model", "inferencePath", "bodyDigest"} {
		a.abortBody[field] = params[field]
	}
	if c.publicStateless {
		secret := params["secret"]
		a.abortBody["secret"] = secret
		if session, _ := rctx.Value(publicCallerSessionContext{}).(string); session != "" {
			a.abortBody["session"] = session
		}
	}
	return a
}

func (a *hostedAdmissionCaller) create(ctx context.Context, params map[string]any, result *hostedDispatch) error {
	if a.client.publicStateless {
		return a.client.publicControl(ctx, "/hosted/dispatches", params, result)
	}
	return a.client.hostedRemote(ctx, "/dispatches", a.authorization, a.key, params, result)
}

func explicitHostedRejection(err error) bool {
	var private *hostedControlHTTPError
	var public *publicControlHTTPError
	return errors.As(err, &private) && private.status >= 400 && private.status < 500 || errors.As(err, &public) && public.status >= 400 && public.status < 500
}

func (a *hostedAdmissionCaller) abort(ctx context.Context) error {
	cleanup, stop := hostedAbortContext(ctx)
	defer stop()
	var ack struct {
		Accepted bool `json:"accepted"`
	}
	var err error
	if a.client.publicStateless {
		err = a.client.publicControl(cleanup, "/hosted/dispatch-attempts/abort", a.abortBody, &ack)
	} else {
		err = a.client.hostedRemote(cleanup, "/dispatch-attempts/abort", a.authorization, a.key, a.abortBody, &ack)
	}
	if err != nil || !ack.Accepted {
		return errors.New("hosted admission cleanup pending")
	}
	return nil
}

func hostedAbortContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
}
