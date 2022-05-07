TARGETS := clean test build
PROJECTS := proj1 proj2 proj3 proj4 proj5

$(TARGETS): $(PROJECTS)

$(PROJECTS):
	$(MAKE) -C $@ $(MAKECMDGOALS)

.PHONY: $(TARGETS) $(PROJECTS)