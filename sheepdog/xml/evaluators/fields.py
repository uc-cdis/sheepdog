from sheepdog.xml.evaluators import Evaluator


class BasicEvaluator(Evaluator):
    """
        Basic element evaluator that expects a single entry for the path, raises an
        Exception when multiple entries are found
    """

    def _evaluate(self):
        """ Searches sml root for the property path and raises an exceoption if multiple paths are found
        Returns:
             Any: text value for path in the xml
        Raises:
            Exception: when multiple values are found for path
        """

        # returns either a value or a list of elements
        value_or_elements = self.search_path()

        if isinstance(value_or_elements, list):
            if len(value_or_elements) > 1:
                raise Exception("More than one {} is found".format(self.path))

            return value_or_elements[0].text if value_or_elements else None
        return value_or_elements


class FilterElementEvaluator(Evaluator):
    """Evaluator that does a search and picks the the first one the matches the criteria no mater the order"""

    def _evaluate(self):
        elements = self.root_element.xpath(self.path, namespaces=self.xml_namespaces)
        if elements:
            return elements[0].text
        return None


class LastFollowUpEvaluator(Evaluator):
    """
        Evaluates the last follow up value using the following criteria
        The desired value is the value of the xml element with the maximum value, if two or more elements share
        this maximum value, use the `sequence` attribute on the parent to break the tieL meaning the the
        parent element with highest sequence will be used
    """

    def _evaluate(self):

        max_element = self.get_max_element(self.path)
        if max_element is not None:
            return max_element.text
        return None

    def get_max_element(self, path):
        tie_breaker = "sequence"
        elements = self.search_path(path)

        if not isinstance(elements, list):
            raise ValueError("path expression must produce a list and not a single value")

        _max, _max_element = None, None
        for element in elements:
            if element.text > _max:
                _max = element.text
                _max_element = element
            elif element.text == _max:
                # break tie
                parent = element.getparent()
                b1 = parent.get(tie_breaker)

                b2 = _max_element.getparent().get(tie_breaker)

                if b1 > b2:
                    _max_element = element

        return _max_element


class VitalStatusEvaluator(LastFollowUpEvaluator):
    """
        Evaluates vital status by using the value of the vital status element that aligns with the constraints of
        the follow up evaluator
    """

    def _evaluate(self):
        elements = self.get_elements()
        if elements:
            return elements[0].text
        return None

    def get_elements(self):
        # locate max days_to_last_follow_up
        path = self.get_evaluator_property("follow_up_path")
        max_element = self.get_max_element(path)

        max_element = max_element.getparent() if max_element is not None else self.root_element
        return max_element.xpath(self.path, namespaces=self.xml_namespaces)


class TreatmentTherapyEvaluator(Evaluator):
    """ Computes tratment of therapy value set in the API, also sets the treatment_type"""

    def evaluate(self):
        return self._evaluate()

    def search(self, root, path, nullable=True):
        val = "not reported"
        elements = root.xpath(path, namespaces=self.xml_namespaces, nullable=nullable)
        if [e for e in elements if e.text is not None and e.text.lower() == "yes"]:
            val = "yes"
        elif [e for e in elements if e.text is not None and e.text.lower() == "no"]:
            val = "no"
        return val

    def _evaluate(self):

        # list type expected
        xpaths = self.path if isinstance(self.path, list) else [self.path] # type: list[str]
        treatments = [None, None]
        for xpath in xpaths:
            val = self.search(self.root_element, xpath)

            if self.is_radiation(xpath):
                val = self.search_additional_paths(val, self.get_evaluator_property("additional_radiation_path"))
                treatments[0] = dict(treatment_type="Radiation, NOS", treatment_or_therapy=val)
            else:
                val = self.search_additional_paths(val, self.get_evaluator_property("additional_pharmaceutical_path"))
                treatments[1] = dict(treatment_type="Pharmaceutical Therapy, NOS", treatment_or_therapy=val)

        return treatments

    def search_additional_paths(self, val, additional_path):

        if val == "yes":
            return val

        allowed_events = self.get_evaluator_property("allowed_tumor_events")
        # search for new_tumor_event_type
        new_tumor_event_path = self.get_evaluator_property("new_tumor_event_path")
        tumor_events = self.search_path(new_tumor_event_path)
        if tumor_events:
            for evt in tumor_events:
                if evt.text in allowed_events:
                    # search parent for yes
                    evt.getparent()
                    val = self.search(evt.getparent(), additional_path)
                    if val == "yes":
                        break
        return val

    def is_radiation(self, path):
        return "radiation_therapy" in path
