cube.piece.type.text = function(board) {
  var timeout;

  var text = cube.piece(board)
      .on("size", resize)
      .on("serialize", serialize)
      .on("deserialize", deserialize);

  var div = d3.select(text.node())
      .classed("text", true);

  if (mode == "edit") {
    div.append("h3")
        .attr("class", "title")
        .text("Text Label");

    var content = div.append("textarea")
        .attr("class", "content")
        .attr("placeholder", "text content…")
        .on("keyup.text", textchange)
        .on("focus.text", text.focus)
        .on("blur.text", text.blur);
  }

  function resize() {
    var transition = text.transition(),
        innerSize = text.innerSize();

    if (mode == "edit") {
      transition.select(".content")
          .style("width", innerSize[0] - 12 + "px")
          .style("height", innerSize[1] - 34 + "px");
    } else {
      transition
          .style("font-size", innerSize[0] / 12 + "px")
          .style("margin-top", innerSize[1] / 2 + innerSize[0] / 5 - innerSize[0] / 12 + "px");
    }
  }

  function textchange() {
    if (timeout) clearTimeout(timeout);
    timeout = setTimeout(text.edit, 750);
  }

  function serialize(json) {
    json.type = "text";
    json.content = content.property("value");
  }

  function deserialize(json) {
    if (mode == "edit") {
      content.property("value", json.content);
    } else {
      div.text(json.content);
    }
  }

  text.copy = function() {
    return board.add(cube.piece.type.text);
  };

  resize();

  return text;
};
