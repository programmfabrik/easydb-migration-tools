PY_DIR=$(CURDIR)/json_migration
DOC_GEN=$(PY_DIR)/_docs_sphinx
DOC=$(CURDIR)/doku/json_migration


clean-docs:
	rm -rf $(DOC_GEN) || true
	rm -rf $(DOC) || true

# requires sphinx-apidoc
# pip3 install sphinx
# pip3 install sphinx-markdown-builder
# pip3 install sphinx-autodoc-typehints
docs:

	sphinx-apidoc -o $(DOC_GEN) $(PY_DIR) sphinx-apidoc --full --no-toc --separate --module-first -A 'Programmfabrik 2021'

	echo " \n\
 	\n\
	import os \n\
	import sys \n\
 	\n\
	sys.path.insert(0, os.path.abspath('../')) \n\
 	\n\
	def skip(app, what, name, obj, would_skip, options): \n\
		if name in ( '__init__',): \n\
			return False \n\
		return would_skip \n\
 	\n\
	def setup(app): \n\
		app.connect('autodoc-skip-member', skip) \n\
 	\n\
	extensions.append('sphinx_autodoc_typehints') \n\
	" >> $(DOC_GEN)/conf.py

	make markdown -C $(DOC_GEN)

	mkdir -p $(DOC)

	cp -v $(DOC_GEN)/_build/markdown/*.md $(DOC)

	# rm -rf $(DOC_GEN) || true

all: clean-docs docs
