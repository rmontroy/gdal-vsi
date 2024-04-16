#
# CPL (Common Portability Library) makefile
#
ifneq ($(wildcard ../GDALmake.op?),)
include ../GDALmake.opt
else
include GDALmake.opt
endif

ifeq ($(LIBZ_SETTING),internal)
XTRA_OPT =      -I../frmts/zlib
else
XTRA_OPT =
endif

CPPFLAGS	:= $(CPPFLAGS)

CXXFLAGS	:=	$(WARN_EFFCPLUSPLUS) $(WARN_OLD_STYLE_CAST) $(CXXFLAGS)

OBJ =	cpl_conv.o cpl_error.o cpl_string.o cplstringlist.o \
	cpl_strtod.o cpl_path.o cpl_minixml.o \
	cpl_multiproc.o cplstring.o \
	cpl_vsisimple.o cpl_vsil.o \
	cpl_vsil_unix_stdio_64.o cpl_http.o \
	cpl_recode.o cpl_recode_iconv.o cpl_recode_stub.o \
	cpl_time.o \
	cpl_vsil_curl.o \
	cpl_vsil_s3.o \
	cpl_aws.o cpl_vsi_error.o 


ifeq ($(CURL_SETTING),yes)
CPPFLAGS	:=	$(CPPFLAGS) -DHAVE_CURL
endif


ifeq ($(HAVE_CRYPTOPP),yes)
CPPFLAGS	:=	$(CPPFLAGS) -DHAVE_CRYPTOPP
endif

ifeq ($(USE_ONLY_CRYPTODLL_ALG),yes)
CPPFLAGS	:=	$(CPPFLAGS) -DUSE_ONLY_CRYPTODLL_ALG
endif

ifeq ($(HAVE_OPENSSL_CRYPTO),yes)
CPPFLAGS	:=	$(CPPFLAGS) -DHAVE_OPENSSL_CRYPTO
endif


default:	$(OBJ:.o=.$(OBJ_EXT)) cpl_sha256.$(OBJ_EXT)

# cpl_sha256.cpp and cpl_vsil_crypt.cpp uses cryptocpp headers which make use of
# unsigned integer overflow arithmetic.
CXXFLAGS_NO_SANITIZE_UIO := $(subst -fsanitize=unsigned-integer-overflow,,${CXXFLAGS})

cpl_sha256.$(OBJ_EXT):   cpl_sha256.cpp
	$(CXX) $(GDAL_INCLUDE) $(CXXFLAGS_NO_SANITIZE_UIO) $(CPPFLAGS) -c -o $@ $<

# cpl_vsil_crypt.$(OBJ_EXT):   cpl_vsil_crypt.cpp
# 	$(CXX) $(GDAL_INCLUDE) $(CXXFLAGS_NO_SANITIZE_UIO) $(CPPFLAGS) -c -o $@ $<


$(OBJ): cpl_vsi_virtual.h cpl_vsil_curl_class.h

clean:
	$(RM) *.o $(O_OBJ)

INST_H_FILES = \
	cpl_atomic_ops.h \
	cpl_config_extras.h \
	cpl_config.h \
	cpl_conv.h \
	cpl_csv.h \
	cpl_error.h \
	cpl_hash_set.h \
	cpl_http.h \
        cpl_json.h \
	cplkeywordparser.h \
	cpl_list.h \
	cpl_minixml.h \
	cpl_minizip_ioapi.h \
	cpl_minizip_unzip.h \
	cpl_minizip_zip.h \
	cpl_multiproc.h \
	cpl_odbc.h \
	cpl_port.h \
	cpl_progress.h \
	cpl_quad_tree.h \
	cpl_spawn.h \
	cpl_string.h \
	cpl_time.h \
	cpl_virtualmem.h \
	cpl_vsi.h \
	cpl_vsi_error.h \
	cpl_vsi_virtual.h \
	gdal_csv.h \
	cpl_auto_close.h

install:
	for f in $(INST_H_FILES) ; do $(INSTALL_DATA) $$f $(DESTDIR)$(INST_INCLUDE) ; done

