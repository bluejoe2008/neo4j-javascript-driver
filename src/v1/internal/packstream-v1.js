/**
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import {utf8} from './node';
import Integer, {int, isInt} from '../integer';
import {newError, PROTOCOL_ERROR} from './../error';
import {Chunker} from './chunking';
import {Node, Path, PathSegment, Relationship, UnboundRelationship} from '../graph-types';
import {BigNumber} from 'bignumber.js';

const TINY_STRING = 0x80;
const TINY_LIST = 0x90;
const TINY_MAP = 0xA0;
const TINY_STRUCT = 0xB0;
const NULL = 0xC0;
const FLOAT_64 = 0xC1;
const FALSE = 0xC2;
const TRUE = 0xC3;
const INT_8 = 0xC8;
const INT_16 = 0xC9;
const INT_32 = 0xCA;
const INT_64 = 0xCB;
const STRING_8 = 0xD0;
const STRING_16 = 0xD1;
const STRING_32 = 0xD2;
const LIST_8 = 0xD4;
const LIST_16 = 0xD5;
const LIST_32 = 0xD6;
const BYTES_8 = 0xCC;
const BYTES_16 = 0xCD;
const BYTES_32 = 0xCE;
const MAP_8 = 0xD8;
const MAP_16 = 0xD9;
const MAP_32 = 0xDA;
const STRUCT_8 = 0xDC;
const STRUCT_16 = 0xDD;

const NODE = 0x4E;
const NODE_STRUCT_SIZE = 3;

const RELATIONSHIP = 0x52;
const RELATIONSHIP_STRUCT_SIZE = 5;

const UNBOUND_RELATIONSHIP = 0x72;
const UNBOUND_RELATIONSHIP_STRUCT_SIZE = 3;

const PATH = 0x50;
const PATH_STRUCT_SIZE = 3;

/**
  * A Structure have a signature and fields.
  * @access private
  */
class Structure {
  /**
   * Create new instance
   */
  constructor( signature, fields ) {
    this.signature = signature;
    this.fields = fields;
  }

  toString() {
    let fieldStr = "";
    for (var i = 0; i < this.fields.length; i++) {
      if(i > 0) { fieldStr+=", " }
      fieldStr += this.fields[i];
    }
    return "Structure(" + this.signature + ", [" + this.fields + "])"
  }
}

/**
  * Class to pack
  * @access private
  */
class Packer {

  /**
   * @constructor
   * @param {Chunker} channel the chunker backed by a network channel.
   */
  constructor(channel) {
    this._ch = channel;
    this._byteArraysSupported = true;
  }

  /**
   * Creates a packable function out of the provided value
   * @param x the value to pack
   * @param onError callback for the case when value cannot be packed
   * @returns Function
   */
  packable (x, onError) {
    if (x === null) {
      return () => this._ch.writeUInt8( NULL );
    } else if (x === true) {
      return () => this._ch.writeUInt8( TRUE );
    } else if (x === false) {
      return () => this._ch.writeUInt8( FALSE );
    } else if (typeof(x) == "number") {
      return () => this.packFloat(x);
    } else if (typeof(x) == "string") {
      return () => this.packString(x, onError);
    } else if (isInt(x)) {
      return () => this.packInteger(x);
    } else if (x instanceof Int8Array) {
      return () => this.packBytes(x, onError);
    } else if (x instanceof Array) {
      return () => {
        this.packListHeader(x.length, onError);
        for (let i = 0; i < x.length; i++) {
          this.packable(x[i] === undefined ? null : x[i], onError)();
        }
      }
    } else if (isIterable(x)) {
      return this.packableIterable(x, onError);
    } else if (x instanceof Node) {
      return this._nonPackableValue(`It is not allowed to pass nodes in query parameters, given: ${x}`, onError);
    } else if (x instanceof Relationship) {
      return this._nonPackableValue(`It is not allowed to pass relationships in query parameters, given: ${x}`, onError);
    } else if (x instanceof Path) {
      return this._nonPackableValue(`It is not allowed to pass paths in query parameters, given: ${x}`, onError);
    } else if (x instanceof Structure) {
      var packableFields = [];
      for (var i = 0; i < x.fields.length; i++) {
        packableFields[i] = this.packable(x.fields[i], onError);
      }
      return () => this.packStruct( x.signature, packableFields );
    } else if (typeof(x) == "object") {
      return () => {
        let keys = Object.keys(x);

        let count = 0;
        for (let i = 0; i < keys.length; i++) {
          if (x[keys[i]] !== undefined) {
            count++;
          }
        }
        this.packMapHeader(count, onError);
        for (let i = 0; i < keys.length; i++) {
          let key = keys[i];
          if (x[key] !== undefined) {
            this.packString(key);
            this.packable(x[key], onError)();
          }
        }
      };
    } else {
      return this._nonPackableValue(`Unable to pack the given value: ${x}`, onError);
    }
  }

  packableIterable(iterable, onError) {
    try {
      const array = Array.from(iterable);
      return this.packable(array, onError);
    } catch (e) {
      // handle errors from iterable to array conversion
      onError(newError(`Cannot pack given iterable, ${e.message}: ${iterable}`));
    }
  }

  /**
   * Packs a struct
   * @param signature the signature of the struct
   * @param packableFields the fields of the struct, make sure you call `packable on all fields`
   */
  packStruct ( signature, packableFields, onError) {
    packableFields = packableFields || [];
    this.packStructHeader(packableFields.length, signature, onError);
    for(let i = 0; i < packableFields.length; i++) {
      packableFields[i]();
    }
  }
  packInteger (x) {
    var high = x.high,
        low  = x.low;

    if (x.greaterThanOrEqual(-0x10) && x.lessThan(0x80)) {
      this._ch.writeInt8(low);
    }
    else if (x.greaterThanOrEqual(-0x80) && x.lessThan(-0x10)) {
      this._ch.writeUInt8(INT_8);
      this._ch.writeInt8(low);
    }
    else if (x.greaterThanOrEqual(-0x8000) && x.lessThan(0x8000)) {
      this._ch.writeUInt8(INT_16);
      this._ch.writeInt16(low);
    }
    else if (x.greaterThanOrEqual(-0x80000000) && x.lessThan(0x80000000)) {
      this._ch.writeUInt8(INT_32);
      this._ch.writeInt32(low);
    }
    else {
      this._ch.writeUInt8(INT_64);
      this._ch.writeInt32(high);
      this._ch.writeInt32(low);
    }
  }

  packFloat(x) {
    this._ch.writeUInt8(FLOAT_64);
    this._ch.writeFloat64(x);
  }

  packString (x, onError) {
    let bytes = utf8.encode(x);
    let size = bytes.length;
    if (size < 0x10) {
      this._ch.writeUInt8(TINY_STRING | size);
      this._ch.writeBytes(bytes);
    } else if (size < 0x100) {
      this._ch.writeUInt8(STRING_8)
      this._ch.writeUInt8(size);
      this._ch.writeBytes(bytes);
    } else if (size < 0x10000) {
      this._ch.writeUInt8(STRING_16);
      this._ch.writeUInt8(size/256>>0);
      this._ch.writeUInt8(size%256);
      this._ch.writeBytes(bytes);
    } else if (size < 0x100000000) {
      this._ch.writeUInt8(STRING_32);
      this._ch.writeUInt8((size/16777216>>0)%256);
      this._ch.writeUInt8((size/65536>>0)%256);
      this._ch.writeUInt8((size/256>>0)%256);
      this._ch.writeUInt8(size%256);
      this._ch.writeBytes(bytes);
    } else {
      onError(newError("UTF-8 strings of size " + size + " are not supported"));
    }
  }

  packListHeader (size, onError) {
    if (size < 0x10) {
      this._ch.writeUInt8(TINY_LIST | size);
    } else if (size < 0x100) {
      this._ch.writeUInt8(LIST_8)
      this._ch.writeUInt8(size);
    } else if (size < 0x10000) {
      this._ch.writeUInt8(LIST_16);
      this._ch.writeUInt8((size/256>>0)%256);
      this._ch.writeUInt8(size%256);
    } else if (size < 0x100000000) {
      this._ch.writeUInt8(LIST_32);
      this._ch.writeUInt8((size/16777216>>0)%256);
      this._ch.writeUInt8((size/65536>>0)%256);
      this._ch.writeUInt8((size/256>>0)%256);
      this._ch.writeUInt8(size%256);
    } else {
      onError(newError("Lists of size " + size + " are not supported"));
    }
  }

  packBytes(array, onError) {
    if(this._byteArraysSupported) {
      this.packBytesHeader(array.length, onError);
      for (let i = 0; i < array.length; i++) {
        this._ch.writeInt8(array[i]);
      }
    }else {
      onError(newError("Byte arrays are not supported by the database this driver is connected to"));
    }
  }

  packBytesHeader(size, onError) {
    if (size < 0x100) {
      this._ch.writeUInt8(BYTES_8);
      this._ch.writeUInt8(size);
    } else if (size < 0x10000) {
      this._ch.writeUInt8(BYTES_16);
      this._ch.writeUInt8((size / 256 >> 0) % 256);
      this._ch.writeUInt8(size % 256);
    } else if (size < 0x100000000) {
      this._ch.writeUInt8(BYTES_32);
      this._ch.writeUInt8((size / 16777216 >> 0) % 256);
      this._ch.writeUInt8((size / 65536 >> 0) % 256);
      this._ch.writeUInt8((size / 256 >> 0) % 256);
      this._ch.writeUInt8(size % 256);
    } else {
      onError(newError('Byte arrays of size ' + size + ' are not supported'));
    }
  }

  packMapHeader (size, onError) {
    if (size < 0x10) {
      this._ch.writeUInt8(TINY_MAP | size);
    } else if (size < 0x100) {
      this._ch.writeUInt8(MAP_8);
      this._ch.writeUInt8(size);
    } else if (size < 0x10000) {
      this._ch.writeUInt8(MAP_16);
      this._ch.writeUInt8(size/256>>0);
      this._ch.writeUInt8(size%256);
    } else if (size < 0x100000000) {
      this._ch.writeUInt8(MAP_32);
      this._ch.writeUInt8((size/16777216>>0)%256);
      this._ch.writeUInt8((size/65536>>0)%256);
      this._ch.writeUInt8((size/256>>0)%256);
      this._ch.writeUInt8(size%256);
    } else {
      onError(newError("Maps of size " + size + " are not supported"));
    }
  }

  packStructHeader (size, signature, onError) {
    if (size < 0x10) {
      this._ch.writeUInt8(TINY_STRUCT | size);
      this._ch.writeUInt8(signature);
    } else if (size < 0x100) {
      this._ch.writeUInt8(STRUCT_8);
      this._ch.writeUInt8(size);
      this._ch.writeUInt8(signature);
    } else if (size < 0x10000) {
      this._ch.writeUInt8(STRUCT_16);
      this._ch.writeUInt8(size/256>>0);
      this._ch.writeUInt8(size%256);
    } else {
      onError(newError("Structures of size " + size + " are not supported"));
    }
  }

  disableByteArrays() {
    this._byteArraysSupported = false;
  }

  _nonPackableValue(message, onError) {
    if (onError) {
      onError(newError(message, PROTOCOL_ERROR));
    }
    return () => undefined;
  }
}

/**
  * Class to unpack
  * @access private
  */
class Unpacker {
  _readUInt16s(buffer, num) {
    var vv = [];
    for(var i = 0; i < num; i++){
      vv.push(buffer.readUInt16());
    }

    return vv;
  }

  _unpackBlob(marker, buffer) {
    //BOLT_VALUE_TYPE_BLOB_REMOTE
    if (marker == 0xC4 || marker == 0xC5) {
      var l1 = buffer.readInt64();
      var l2 = this._readUInt16s(buffer, 4);
      //[llll,llll][llll,llll][llll,llll][llll,llll][llll,llll][llll,llll][mmmm,mmmm][mmmm,mmmm] (l=length, m=mimeType)

      const BigNumber = require("bignumber.js");
      //var length = l2[0] << 32 | l2[1] << 16 | l2[2];
      var length = parseInt(parseInt(
        new BigNumber(l2[0]).times(256).times(256).times(256).times(256)
        .plus(new BigNumber(l2[1]).times(256).times(256))
        .plus(l2[2])
        .toNumber()));

      var mimeNames = {'-1':'unknown/unknown','1':'application/acad','2':'application/arj','3':'application/base64','4':'application/binhex','5':'application/binhex4','6':'application/book','7':'application/cdf','8':'application/clariscad','9':'application/commonground','10':'application/drafting','11':'application/dsptype','12':'application/dxf','13':'application/ecmascript','14':'application/envoy','15':'application/excel','16':'application/fractals','17':'application/freeloader','18':'application/futuresplash','19':'application/gnutar','20':'application/groupwise','21':'application/hlp','22':'application/hta','23':'application/i-deas','24':'application/iges','25':'application/inf','26':'application/java','27':'application/java-byte-code','28':'application/javascript','29':'application/lha','30':'application/lzx','31':'application/macbinary','32':'application/mac-binary','33':'application/mac-binhex','34':'application/mac-binhex40','35':'application/mac-compactpro','36':'application/marc','37':'application/mbedlet','38':'application/mcad','39':'application/mime','40':'application/mspowerpoint','41':'application/msword','42':'application/mswrite','43':'application/netmc','44':'application/octet-stream','45':'application/oda','46':'application/pdf','47':'application/pkcs10','48':'application/pkcs-12','49':'application/pkcs7-mime','50':'application/pkcs7-signature','51':'application/pkcs-crl','52':'application/pkix-cert','53':'application/pkix-crl','54':'application/plain','55':'application/postscript','56':'application/powerpoint','57':'application/pro_eng','58':'application/ringing-tones','59':'application/rtf','60':'application/sdp','61':'application/sea','62':'application/set','63':'application/sla','64':'application/smil','65':'application/solids','66':'application/sounder','67':'application/step','68':'application/streamingmedia','69':'application/toolbook','70':'application/vda','71':'application/vnd.fdf','72':'application/vnd.hp-hpgl','73':'application/vnd.hp-pcl','74':'application/vnd.ms-excel','75':'application/vnd.ms-pki.certstore','76':'application/vnd.ms-pki.pko','77':'application/vnd.ms-pki.seccat','78':'application/vnd.ms-pki.stl','79':'application/vnd.ms-powerpoint','80':'application/vnd.ms-project','81':'application/vnd.nokia.configuration-message','82':'application/vnd.nokia.ringing-tone','83':'application/vnd.rn-realmedia','84':'application/vnd.rn-realplayer','85':'application/vnd.wap.wmlc','86':'application/vnd.wap.wmlscriptc','87':'application/vnd.xara','88':'application/vocaltec-media-desc','89':'application/vocaltec-media-file','90':'application/wordperfect','91':'application/wordperfect6.0','92':'application/wordperfect6.1','93':'application/x-123','94':'application/x-aim','95':'application/x-authorware-bin','96':'application/x-authorware-map','97':'application/x-authorware-seg','98':'application/x-bcpio','99':'application/x-binary','100':'application/x-binhex40','101':'application/x-bsh','102':'application/x-bytecode.elisp (compiled elisp)','103':'application/x-bytecode.python','104':'application/x-bzip','105':'application/x-bzip2','106':'application/x-cdf','107':'application/x-cdlink','108':'application/x-chat','109':'application/x-cmu-raster','110':'application/x-cocoa','111':'application/x-compactpro','112':'application/x-compress','113':'application/x-compressed','114':'application/x-conference','115':'application/x-cpio','116':'application/x-cpt','117':'application/x-csh','118':'application/x-deepv','119':'application/x-director','120':'application/x-dvi','121':'application/x-elc','122':'application/x-envoy','123':'application/x-esrehber','124':'application/x-excel','125':'application/x-frame','126':'application/x-freelance','127':'application/x-gsp','128':'application/x-gss','129':'application/x-gtar','130':'application/x-gzip','131':'application/x-hdf','132':'application/x-helpfile','133':'application/x-httpd-imap','134':'application/x-ima','135':'application/x-internett-signup','136':'application/x-inventor','137':'application/x-ip2','138':'application/x-java-class','139':'application/x-java-commerce','140':'application/x-javascript','141':'application/x-koan','142':'application/x-ksh','143':'application/x-latex','144':'application/x-lha','145':'application/x-lisp','146':'application/x-livescreen','147':'application/x-lotus','148':'application/x-lotusscreencam','149':'application/x-lzh','150':'application/x-lzx','151':'application/x-macbinary','152':'application/x-mac-binhex40','153':'application/x-magic-cap-package-1.0','154':'application/x-mathcad','155':'application/x-meme','156':'application/x-midi','157':'application/x-mif','158':'application/x-mix-transfer','159':'application/xml','160':'application/x-mplayer2','161':'application/x-msexcel','162':'application/x-mspowerpoint','163':'application/x-navi-animation','164':'application/x-navidoc','165':'application/x-navimap','166':'application/x-navistyle','167':'application/x-netcdf','168':'application/x-newton-compatible-pkg','169':'application/x-nokia-9000-communicator-add-on-software','170':'application/x-omc','171':'application/x-omcdatamaker','172':'application/x-omcregerator','173':'application/x-pagemaker','174':'application/x-pcl','175':'application/x-pixclscript','176':'application/x-pkcs10','177':'application/x-pkcs12','178':'application/x-pkcs7-certificates','179':'application/x-pkcs7-certreqresp','180':'application/x-pkcs7-mime','181':'application/x-pkcs7-signature','182':'application/x-pointplus','183':'application/x-portable-anymap','184':'application/x-project','185':'application/x-qpro','186':'application/x-rtf','187':'application/x-sdp','188':'application/x-sea','189':'application/x-seelogo','190':'application/x-sh','191':'application/x-shar','192':'application/x-shockwave-flash','193':'application/x-sit','194':'application/x-sprite','195':'application/x-stuffit','196':'application/x-sv4cpio','197':'application/x-sv4crc','198':'application/x-tar','199':'application/x-tbook','200':'application/x-tcl','201':'application/x-tex','202':'application/x-texinfo','203':'application/x-troff','204':'application/x-troff-man','205':'application/x-troff-me','206':'application/x-troff-ms','207':'application/x-troff-msvideo','208':'application/x-ustar','209':'application/x-visio','210':'application/x-vnd.audioexplosion.mzz','211':'application/x-vnd.ls-xpix','212':'application/x-vrml','213':'application/x-wais-source','214':'application/x-winhelp','215':'application/x-wintalk','216':'application/x-world','217':'application/x-wpwin','218':'application/x-wri','219':'application/x-x509-ca-cert','220':'application/x-x509-user-cert','221':'application/x-zip-compressed','222':'application/zip','223':'audio/aiff','224':'audio/basic','225':'audio/it','226':'audio/make','227':'audio/make.my.funk','228':'audio/mid','229':'audio/midi','230':'audio/mod','231':'audio/mpeg','232':'audio/mpeg3','233':'audio/nspaudio','234':'audio/s3m','235':'audio/tsp-audio','236':'audio/tsplayer','237':'audio/vnd.qcelp','238':'audio/voc','239':'audio/voxware','240':'audio/wav','241':'audio/x-adpcm','242':'audio/x-aiff','243':'audio/x-au','244':'audio/x-gsm','245':'audio/x-jam','246':'audio/x-liveaudio','247':'audio/xm','248':'audio/x-mid','249':'audio/x-midi','250':'audio/x-mod','251':'audio/x-mpeg','252':'audio/x-mpeg-3','253':'audio/x-mpequrl','254':'audio/x-nspaudio','255':'audio/x-pn-realaudio','256':'audio/x-pn-realaudio-plugin','257':'audio/x-psid','258':'audio/x-realaudio','259':'audio/x-twinvq','260':'audio/x-twinvq-plugin','261':'audio/x-vnd.audioexplosion.mjuicemediafile','262':'audio/x-voc','263':'audio/x-wav','264':'chemical/x-pdb','265':'drawing/x-dwf (old)','266':'image/bmp','267':'image/cmu-raster','268':'image/fif','269':'image/florian','270':'image/g3fax','271':'image/gif','272':'image/ief','273':'image/jpeg','274':'image/jutvision','275':'image/naplps','276':'image/pict','277':'image/pjpeg','278':'image/png','279':'image/tiff','280':'image/vasa','281':'image/vnd.dwg','282':'image/vnd.fpx','283':'image/vnd.net-fpx','284':'image/vnd.rn-realflash','285':'image/vnd.rn-realpix','286':'image/vnd.wap.wbmp','287':'image/vnd.xiff','288':'image/xbm','289':'image/x-cmu-raster','290':'image/x-dwg','291':'image/x-icon','292':'image/x-jg','293':'image/x-jps','294':'image/x-niff','295':'image/x-pcx','296':'image/x-pict','297':'image/xpm','298':'image/x-portable-anymap','299':'image/x-portable-bitmap','300':'image/x-portable-graymap','301':'image/x-portable-greymap','302':'image/x-portable-pixmap','303':'image/x-quicktime','304':'image/x-rgb','305':'image/x-tiff','306':'image/x-windows-bmp','307':'image/x-xbitmap','308':'image/x-xbm','309':'image/x-xpixmap','310':'image/x-xwd','311':'image/x-xwindowdump','312':'i-world/i-vrml','313':'message/rfc822','314':'model/iges','315':'model/vnd.dwf','316':'model/vrml','317':'model/x-pov','318':'multipart/x-gzip','319':'multipart/x-ustar','320':'multipart/x-zip','321':'music/crescendo','322':'music/x-karaoke','323':'paleovu/x-pv','324':'text/asp','325':'text/css','326':'text/ecmascript','327':'text/html','328':'text/javascript','329':'text/mcf','330':'text/pascal','331':'text/plain','332':'text/richtext','333':'text/scriplet','334':'text/sgml','335':'text/tab-separated-values','336':'text/uri-list','337':'text/vnd.abc','338':'text/vnd.fmi.flexstor','339':'text/vnd.rn-realtext','340':'text/vnd.wap.wml','341':'text/vnd.wap.wmlscript','342':'text/webviewhtml','343':'text/x-asm','344':'text/x-audiosoft-intra','345':'text/x-c','346':'text/x-component','347':'text/x-fortran','348':'text/x-h','349':'text/x-java-source','350':'text/x-la-asf','351':'text/x-m','352':'text/xml','353':'text/x-pascal','354':'text/x-script','355':'text/x-script.csh','356':'text/x-script.elisp','357':'text/x-script.guile','358':'text/x-script.ksh','359':'text/x-script.lisp','360':'text/x-script.perl','361':'text/x-script.perl-module','362':'text/x-script.phyton','363':'text/x-script.rexx','364':'text/x-script.scheme','365':'text/x-script.sh','366':'text/x-script.tcl','367':'text/x-script.tcsh','368':'text/x-script.zsh','369':'text/x-server-parsed-html','370':'text/x-setext','371':'text/x-sgml','372':'text/x-speech','373':'text/x-uil','374':'text/x-uuencode','375':'text/x-vcalendar','376':'video/animaflex','377':'video/avi','378':'video/avs-video','379':'video/dl','380':'video/fli','381':'video/gl','382':'video/mpeg','383':'video/msvideo','384':'video/quicktime','385':'video/vdo','386':'video/vivo','387':'video/vnd.rn-realvideo','388':'video/vnd.vivo','389':'video/vosaic','390':'video/x-amt-demorun','391':'video/x-amt-showrun','392':'video/x-atomic3d-feature','393':'video/x-dl','394':'video/x-dv','395':'video/x-fli','396':'video/x-gl','397':'video/x-isvideo','398':'video/x-motion-jpeg','399':'video/x-mpeg','400':'video/x-mpeq2a','401':'video/x-ms-asf','402':'video/x-ms-asf-plugin','403':'video/x-msvideo','404':'video/x-qtc','405':'video/x-scm','406':'video/x-sgi-movie','407':'windows/metafile','408':'www/mime','409':'x-conference/x-cooltalk','410':'xgl/drawing','411':'xgl/movie','412':'x-music/x-midi','413':'x-world/x-3dmf','414':'x-world/x-svr','415':'x-world/x-vrml','416':'x-world/x-vrt','417':'video/mp4','418':'audio/mp3'};
      var mime = mimeNames["" + parseInt(l2[3])];

      var vv = this._readUInt16s(buffer, 8);
      var bid = this._blobId2String(vv);
      var pretty = {
        '@type': 'blob',
        id: bid,
        length: length,
        mimetype: mime
      };

      return pretty;
    }

    return null;
  }

  _fillString(blank, str) {
    return blank.substring(0, blank.length - str.length).concat(str);
  }

  _blobId2String(vv) {
    return ""
      + this._fillString("0000", vv[0].toString(16))
      + this._fillString("0000", vv[1].toString(16))
      + "-"
      + this._fillString("0000", vv[2].toString(16))
      + "-"
      + this._fillString("0000", vv[3].toString(16))
      + "-"
      + this._fillString("0000", vv[4].toString(16))
      + "-"
      + this._fillString("0000", vv[5].toString(16))
      + this._fillString("0000", vv[6].toString(16))
      + this._fillString("0000", vv[7].toString(16));
  }

  /**
   * @constructor
   * @param {boolean} disableLosslessIntegers if this unpacker should convert all received integers to native JS numbers.
   */
  constructor(disableLosslessIntegers = false) {
    this._disableLosslessIntegers = disableLosslessIntegers;
  }

  unpack(buffer) {
    const marker = buffer.readUInt8();
    const markerHigh = marker & 0xF0;
    const markerLow = marker & 0x0F;

    if (marker == NULL) {
      return null;
    }

    const boolean = this._unpackBoolean(marker);
    if (boolean !== null) {
      return boolean;
    }

		const blob = this._unpackBlob(marker, buffer);
		if (blob != null) {
			return blob;
		}

    const numberOrInteger = this._unpackNumberOrInteger(marker, buffer);
    if (numberOrInteger !== null) {
      if (this._disableLosslessIntegers && isInt(numberOrInteger)) {
        return numberOrInteger.toNumberOrInfinity();
      }
      return numberOrInteger;
    }

    const string = this._unpackString(marker, markerHigh, markerLow, buffer);
    if (string !== null) {
      return string;
    }

    const list = this._unpackList(marker, markerHigh, markerLow, buffer);
    if (list !== null) {
      return list;
    }

    const byteArray = this._unpackByteArray(marker, buffer);
    if (byteArray !== null) {
      return byteArray;
    }

    const map = this._unpackMap(marker, markerHigh, markerLow, buffer);
    if (map !== null) {
      return map;
    }

    const struct = this._unpackStruct(marker, markerHigh, markerLow, buffer);
    if (struct !== null) {
      return struct;
    }

    throw newError('Unknown packed value with marker ' + marker.toString(16));
  }

  unpackInteger(buffer) {
    const marker = buffer.readUInt8();
    const result = this._unpackInteger(marker, buffer);
    if (result == null) {
      throw newError('Unable to unpack integer value with marker ' + marker.toString(16));
    }
    return result;
  }

  _unpackBoolean(marker) {
    if (marker == TRUE) {
      return true;
    } else if (marker == FALSE) {
      return false;
    } else {
      return null;
    }
  }

  _unpackNumberOrInteger(marker, buffer) {
    if (marker == FLOAT_64) {
      return buffer.readFloat64();
    } else {
      return this._unpackInteger(marker, buffer);
    }
  }

  _unpackInteger(marker, buffer) {
    if (marker >= 0 && marker < 128) {
      return int(marker);
    } else if (marker >= 240 && marker < 256) {
      return int(marker - 256);
    } else if (marker == INT_8) {
      return int(buffer.readInt8());
    } else if (marker == INT_16) {
      return int(buffer.readInt16());
    } else if (marker == INT_32) {
      let b = buffer.readInt32();
      return int(b);
    } else if (marker == INT_64) {
      const high = buffer.readInt32();
      const low = buffer.readInt32();
      return new Integer(low, high);
    } else {
      return null;
    }
  }

  _unpackString(marker, markerHigh, markerLow, buffer) {
    if (markerHigh == TINY_STRING) {
      return utf8.decode(buffer, markerLow);
    } else if (marker == STRING_8) {
      return utf8.decode(buffer, buffer.readUInt8());
    } else if (marker == STRING_16) {
      return utf8.decode(buffer, buffer.readUInt16());
    } else if (marker == STRING_32) {
      return utf8.decode(buffer, buffer.readUInt32());
    } else {
      return null;
    }
  }

  _unpackList(marker, markerHigh, markerLow, buffer) {
    if (markerHigh == TINY_LIST) {
      return this._unpackListWithSize(markerLow, buffer);
    } else if (marker == LIST_8) {
      return this._unpackListWithSize(buffer.readUInt8(), buffer);
    } else if (marker == LIST_16) {
      return this._unpackListWithSize(buffer.readUInt16(), buffer);
    } else if (marker == LIST_32) {
      return this._unpackListWithSize(buffer.readUInt32(), buffer);
    } else {
      return null;
    }
  }

  _unpackListWithSize(size, buffer) {
    let value = [];
    for (let i = 0; i < size; i++) {
      value.push(this.unpack(buffer));
    }
    return value;
  }

	_unpackByteArray(marker, buffer) {
		if (marker == BYTES_8) {
			return this._unpackByteArrayWithSize(buffer.readUInt8(), buffer);
		} else if (marker == BYTES_16) {
			return this._unpackByteArrayWithSize(buffer.readUInt16(), buffer);
		} else if (marker == BYTES_32) {
			return this._unpackByteArrayWithSize(buffer.readUInt32(), buffer);
		} else {
			return null;
		}
	}

  _unpackByteArrayWithSize(size, buffer) {
    const value = new Int8Array(size);
    for (let i = 0; i < size; i++) {
      value[i] = buffer.readInt8();
    }
    return value;
  }

  _unpackMap(marker, markerHigh, markerLow, buffer) {
    if (markerHigh == TINY_MAP) {
      return this._unpackMapWithSize(markerLow, buffer);
    } else if (marker == MAP_8) {
      return this._unpackMapWithSize(buffer.readUInt8(), buffer);
    } else if (marker == MAP_16) {
      return this._unpackMapWithSize(buffer.readUInt16(), buffer);
    } else if (marker == MAP_32) {
      return this._unpackMapWithSize(buffer.readUInt32(), buffer);
    } else {
      return null;
    }
  }

  _unpackMapWithSize(size, buffer) {
    let value = {};
    for (let i = 0; i < size; i++) {
      let key = this.unpack(buffer);
      value[key] = this.unpack(buffer);
    }
    return value;
  }

  _unpackStruct(marker, markerHigh, markerLow, buffer) {
    if (markerHigh == TINY_STRUCT) {
      return this._unpackStructWithSize(markerLow, buffer);
    } else if (marker == STRUCT_8) {
      return this._unpackStructWithSize(buffer.readUInt8(), buffer);
    } else if (marker == STRUCT_16) {
      return this._unpackStructWithSize(buffer.readUInt16(), buffer);
    } else {
      return null;
    }
  }

  _unpackStructWithSize(structSize, buffer) {
    const signature = buffer.readUInt8();
    if (signature == NODE) {
      return this._unpackNode(structSize, buffer);
    } else if (signature == RELATIONSHIP) {
      return this._unpackRelationship(structSize, buffer);
    } else if (signature == UNBOUND_RELATIONSHIP) {
      return this._unpackUnboundRelationship(structSize, buffer);
    } else if (signature == PATH) {
      return this._unpackPath(structSize, buffer);
    } else {
      return this._unpackUnknownStruct(signature, structSize, buffer);
    }
  }

  _unpackNode(structSize, buffer) {
    this._verifyStructSize('Node', NODE_STRUCT_SIZE, structSize);

    return new Node(
      this.unpack(buffer), // Identity
      this.unpack(buffer), // Labels
      this.unpack(buffer)  // Properties
    );
  }

  _unpackRelationship(structSize, buffer) {
    this._verifyStructSize('Relationship', RELATIONSHIP_STRUCT_SIZE, structSize);

    return new Relationship(
      this.unpack(buffer), // Identity
      this.unpack(buffer), // Start Node Identity
      this.unpack(buffer), // End Node Identity
      this.unpack(buffer), // Type
      this.unpack(buffer)  // Properties
    );
  }

  _unpackUnboundRelationship(structSize, buffer) {
    this._verifyStructSize('UnboundRelationship', UNBOUND_RELATIONSHIP_STRUCT_SIZE, structSize);

    return new UnboundRelationship(
      this.unpack(buffer), // Identity
      this.unpack(buffer), // Type
      this.unpack(buffer)  // Properties
    );
  }

  _unpackPath(structSize, buffer) {
    this._verifyStructSize('Path', PATH_STRUCT_SIZE, structSize);

    const nodes = this.unpack(buffer);
    const rels = this.unpack(buffer);
    const sequence = this.unpack(buffer);

    const segments = [];
    let prevNode = nodes[0];

    for (let i = 0; i < sequence.length; i += 2) {
      const nextNode = nodes[sequence[i + 1]];
      let relIndex = sequence[i];
      let rel;

      if (relIndex > 0) {
        rel = rels[relIndex - 1];
        if (rel instanceof UnboundRelationship) {
          // To avoid duplication, relationships in a path do not contain
          // information about their start and end nodes, that's instead
          // inferred from the path sequence. This is us inferring (and,
          // for performance reasons remembering) the start/end of a rel.
          rels[relIndex - 1] = rel = rel.bind(prevNode.identity, nextNode.identity);
        }
      } else {
        rel = rels[-relIndex - 1];
        if (rel instanceof UnboundRelationship) {
          // See above
          rels[-relIndex - 1] = rel = rel.bind(nextNode.identity, prevNode.identity);
        }
      }
      // Done hydrating one path segment.
      segments.push(new PathSegment(prevNode, rel, nextNode));
      prevNode = nextNode;
    }
    return new Path(nodes[0], nodes[nodes.length - 1], segments);
  }

  _unpackUnknownStruct(signature, structSize, buffer) {
    const result = new Structure(signature, []);
    for (let i = 0; i < structSize; i++) {
      result.fields.push(this.unpack(buffer));
    }
    return result;
  }

  _verifyStructSize(structName, expectedSize, actualSize) {
    if (expectedSize !== actualSize) {
      throw newError(`Wrong struct size for ${structName}, expected ${expectedSize} but was ${actualSize}`, PROTOCOL_ERROR);
    }
  }
}

function isIterable(obj) {
  if (obj == null) {
    return false;
  }
  return typeof obj[Symbol.iterator] === 'function';
}

export {
  Packer,
  Unpacker,
  Structure
};
