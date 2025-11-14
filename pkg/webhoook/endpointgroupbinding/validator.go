package endpointgroupbinding

import (
	"encoding/json"
	"fmt"
	"net/http"

	endpointgroupbindingv1alpha1 "github.com/h3poteto/aws-global-accelerator-controller/pkg/apis/endpointgroupbinding/v1alpha1"
	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	klog "k8s.io/klog/v2"
)

func Validate(admission *admissionv1.AdmissionReview) *admissionv1.AdmissionReview {
	if admission.Request.Kind.Kind != "EndpointGroupBinding" {
		err := fmt.Errorf("%s is not supported", admission.Request.Kind.Kind)
		klog.Error(err)
		return reviewResponse(admission.Request.UID, false, 400, err.Error())
	}

	if admission.Request.Operation == admissionv1.Create {
		new := endpointgroupbindingv1alpha1.EndpointGroupBinding{}
		if err := json.Unmarshal(admission.Request.Object.Raw, &new); err != nil {
			klog.Error(err)
			return reviewResponse(admission.Request.UID, false, http.StatusInternalServerError, err.Error())
		}

		allowed, err := validateCreate(&new)
		if err != nil {
			klog.Error(err)
			return reviewResponse(admission.Request.UID, false, http.StatusForbidden, err.Error())
		}

		return reviewResponse(admission.Request.UID, allowed, http.StatusOK, "valid")
	}

	if admission.Request.Operation != admissionv1.Update {
		klog.V(4).Info("Operation is not Update or Create")
		return reviewResponse(admission.Request.UID, true, http.StatusOK, "")
	}

	if admission.Request.OldObject.Raw == nil {
		klog.V(4).Info("OldObject is nil")
		return reviewResponse(admission.Request.UID, true, http.StatusOK, "")
	}

	previous := endpointgroupbindingv1alpha1.EndpointGroupBinding{}
	new := endpointgroupbindingv1alpha1.EndpointGroupBinding{}
	if err := json.Unmarshal(admission.Request.OldObject.Raw, &previous); err != nil {
		klog.Error(err)
		return reviewResponse(admission.Request.UID, false, http.StatusInternalServerError, err.Error())
	}

	if err := json.Unmarshal(admission.Request.Object.Raw, &new); err != nil {
		klog.Error(err)
		return reviewResponse(admission.Request.UID, false, http.StatusInternalServerError, err.Error())
	}

	allowed, err := validateUpdate(&previous, &new)
	if err != nil {
		klog.Error(err)
		return reviewResponse(admission.Request.UID, false, http.StatusForbidden, err.Error())
	}

	return reviewResponse(admission.Request.UID, allowed, http.StatusOK, "valid")
}

func validateCreate(new *endpointgroupbindingv1alpha1.EndpointGroupBinding) (bool, error) {
	// Validate that at least one ARN is specified
	if new.Spec.EndpointGroupArn == "" && new.Spec.AcceleratorArn == "" {
		return false, fmt.Errorf("either Spec.EndpointGroupArn or Spec.AcceleratorArn must be specified")
	}

	// Validate that both ARNs are not specified
	if new.Spec.EndpointGroupArn != "" && new.Spec.AcceleratorArn != "" {
		return false, fmt.Errorf("Spec.EndpointGroupArn and Spec.AcceleratorArn are mutually exclusive")
	}

	return true, nil
}

func validateUpdate(previous, new *endpointgroupbindingv1alpha1.EndpointGroupBinding) (bool, error) {
	// Validate immutability
	if previous.Spec.EndpointGroupArn != new.Spec.EndpointGroupArn {
		return false, fmt.Errorf("Spec.EndpointGroupArn is immutable")
	}
	if previous.Spec.AcceleratorArn != new.Spec.AcceleratorArn {
		return false, fmt.Errorf("Spec.AcceleratorArn is immutable")
	}

	// Validate that at least one ARN is specified
	if new.Spec.EndpointGroupArn == "" && new.Spec.AcceleratorArn == "" {
		return false, fmt.Errorf("either Spec.EndpointGroupArn or Spec.AcceleratorArn must be specified")
	}

	// Validate that both ARNs are not specified
	if new.Spec.EndpointGroupArn != "" && new.Spec.AcceleratorArn != "" {
		return false, fmt.Errorf("Spec.EndpointGroupArn and Spec.AcceleratorArn are mutually exclusive")
	}

	return true, nil
}

func reviewResponse(uid types.UID, allowed bool, code int32, reason string) *admissionv1.AdmissionReview {
	return &admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{
			Kind:       "AdmissionReview",
			APIVersion: "admission.k8s.io/v1",
		},
		Response: &admissionv1.AdmissionResponse{
			UID:     uid,
			Allowed: allowed,
			Result: &metav1.Status{
				Code:    code,
				Message: reason,
			},
		},
	}

}
