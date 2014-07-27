// Copyright (c) 2012, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

import "dart:html";

ButtonElement genButton;

void generateBadge(Event e) {
  
}

void main() {
  querySelector('#inputName');
  genButton = querySelector('#generateButton');
  genButton.onClick.listen(generateBadge);
}
